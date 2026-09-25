/** Text that stands in for a credential removed from a URL. */
const REDACTED_URL_CREDENTIAL = "REDACTED";

const SCHEME_SEPARATOR = /:(?:\\?\/){2}/;
// JSON-escaped text writes each slash as "\/", so the separator allows it. A
// URL may follow any character, such as the "+" of a form-encoded value.
const URL_IN_TEXT = /[a-z][a-z0-9+.-]*:(?:\\?\/){2}[^\s"'<>`]+/gi;
const TRAILING_PUNCTUATION = /[.,;:!?)\]}\\]+$/;
// JSON encoders such as Go's encoding/json and Rails write "&" as "\u0026".
const PARAMETER_SEPARATOR = /(&amp;|\\u0026|&|;)/i;
// A redirect target inside another redirect parameter is encoded twice.
const MAX_NESTED_ENCODINGS = 4;
const JWT_SHAPE = /^[\w-]+\.[\w-]+\.[\w-]*$/;
const TELEGRAM_BOT_TOKEN = /^bot\d+:[\w-]{20,}$/;

const CREDENTIAL_WORDS: ReadonlySet<string> = new Set([
  "apikey",
  "auth",
  "authorization",
  "bearer",
  "cookie",
  "credential",
  "credentials",
  "hmac",
  "jsessionid",
  "jwt",
  "key",
  "passwd",
  "password",
  "phpsessid",
  "pwd",
  "secret",
  "sessionid",
  "sig",
  "signature",
  "token",
]);
// A pagination cursor or a lookup key names data, not a secret, so these
// qualifiers keep "pageToken" or "sort_key" readable in recorded text.
const NON_CREDENTIAL_QUALIFIERS: ReadonlySet<string> = new Set([
  "cache",
  "continuation",
  "cursor",
  "dedup",
  "foreign",
  "hash",
  "idempotency",
  "lookup",
  "next",
  "object",
  "page",
  "pagination",
  "partition",
  "prev",
  "previous",
  "primary",
  "public",
  "range",
  "resume",
  "row",
  "sort",
  "sync",
]);
const SECRET_PATH_PARENTS: ReadonlySet<string> = new Set([
  "access_token",
  "api-key",
  "api_key",
  "apikey",
  "key",
  "password",
  "secret",
  "secrets",
  "sig",
  "signature",
  "token",
  "tokens",
]);

function decodeComponent(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

/** Whether a value is a JSON Web Token, whose header decodes to a JSON object. */
function isJwt(value: string): boolean {
  if (!JWT_SHAPE.test(value)) return false;
  const header = value.slice(0, value.indexOf("."));
  try {
    const decoded = atob(header.replace(/-/g, "+").replace(/_/g, "/"));
    return decoded.startsWith('{"') && decoded.includes('"alg"');
  } catch {
    return false;
  }
}

function getNameWords(name: string): string[] {
  return decodeComponent(name)
    .replace(/^amp;/i, "")
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter(Boolean);
}

/** Whether a query, fragment or path-parameter name carries a credential. */
function isCredentialName(name: string): boolean {
  const words = getNameWords(name);
  const last = words.at(-1);
  if (last === undefined) return false;
  if (last === "id" && words.at(-2) === "session") return true;
  if (!CREDENTIAL_WORDS.has(last)) return false;
  const qualifier = words.at(-2);
  return qualifier === undefined || !NON_CREDENTIAL_QUALIFIERS.has(qualifier);
}

// Object keys hold structured data, so a bare "key", "auth" or "signature"
// field stays readable; only these final words, or "key" after a credential
// qualifier, mark a compound key such as `access_token` or `x-api-key`.
const CREDENTIAL_KEY_WORDS: ReadonlySet<string> = new Set([
  "apikey",
  "authorization",
  "cookie",
  "credential",
  "credentials",
  "passwd",
  "password",
  "secret",
  "token",
]);
const CREDENTIAL_KEY_QUALIFIERS: ReadonlySet<string> = new Set([
  "access",
  "api",
  "auth",
  "client",
  "encryption",
  "master",
  "private",
  "secret",
  "service",
  "signing",
  "subscription",
]);

/** Whether an object key names a credential, such as `accessToken` or `client_secret`. */
export function isCredentialKeyName(name: string): boolean {
  const words = getNameWords(name);
  const last = words.at(-1);
  const qualifier = words.at(-2);
  if (last === undefined) return false;
  if (qualifier !== undefined && NON_CREDENTIAL_QUALIFIERS.has(qualifier))
    return false;
  if (CREDENTIAL_KEY_WORDS.has(last)) return true;
  return (
    last === "key" &&
    qualifier !== undefined &&
    CREDENTIAL_KEY_QUALIFIERS.has(qualifier)
  );
}

function redactNestedValue(value: string): string {
  let decoded = value;
  let encodings = 0;
  while (!SCHEME_SEPARATOR.test(decoded)) {
    const next = decodeComponent(decoded);
    if (next === decoded || encodings === MAX_NESTED_ENCODINGS) return value;
    decoded = next;
    encodings += 1;
  }
  const redacted = redactUrlCredentials(decoded);
  if (redacted === decoded) return value;
  let encoded = redacted;
  for (let layer = 0; layer < encodings; layer += 1)
    encoded = encodeURIComponent(encoded);
  return encoded;
}

function redactParameters(parameters: string): string {
  return parameters
    .split(PARAMETER_SEPARATOR)
    .map((part, index) => {
      if (index % 2 === 1 || part === "") return part;
      const equals = part.indexOf("=");
      if (equals === -1)
        return isJwt(decodeComponent(part)) ? REDACTED_URL_CREDENTIAL : part;
      const name = part.slice(0, equals);
      const value = part.slice(equals + 1);
      if (value === "" || value === REDACTED_URL_CREDENTIAL) return part;
      if (isCredentialName(name) || isJwt(decodeComponent(value)))
        return `${name}=${REDACTED_URL_CREDENTIAL}`;
      return `${name}=${redactNestedValue(value)}`;
    })
    .join("");
}

function redactPath(path: string, host: string): string {
  const segments = path.split("/");
  const names = segments.map((segment) =>
    decodeComponent(segment.replace(/\\$/, "")).toLowerCase(),
  );
  const webhooks = names.indexOf("webhooks");
  const slackServices =
    host.toLowerCase() === "hooks.slack.com" ? names.indexOf("services") : -1;
  return segments
    .map((segment, index) => {
      const jsonEscape = segment.endsWith("\\") ? "\\" : "";
      const bare = segment.slice(0, segment.length - jsonEscape.length);
      const semicolon = bare.indexOf(";");
      const name = semicolon === -1 ? bare : bare.slice(0, semicolon);
      const parameters =
        semicolon === -1
          ? ""
          : `;${redactParameters(bare.slice(semicolon + 1))}`;
      const secret =
        name !== "" &&
        name !== REDACTED_URL_CREDENTIAL &&
        (isJwt(decodeComponent(name)) ||
          TELEGRAM_BOT_TOKEN.test(decodeComponent(name)) ||
          SECRET_PATH_PARENTS.has(names[index - 1] ?? "") ||
          (webhooks !== -1 && index === webhooks + 2) ||
          (slackServices !== -1 && index === slackServices + 3));
      return `${secret ? REDACTED_URL_CREDENTIAL : name}${parameters}${jsonEscape}`;
    })
    .join("/");
}

function redactUrl(candidate: string): string {
  const trailing = TRAILING_PUNCTUATION.exec(candidate)?.[0] ?? "";
  const url = candidate.slice(0, candidate.length - trailing.length);
  const separator = SCHEME_SEPARATOR.exec(url);
  if (!separator) return candidate;
  const prefix = url.slice(0, separator.index + separator[0].length);
  const rest = url.slice(prefix.length);
  const authorityEnd = rest.search(/[/?#\\]/);
  let authority = authorityEnd === -1 ? rest : rest.slice(0, authorityEnd);
  const tail = authorityEnd === -1 ? "" : rest.slice(authorityEnd);
  const at = authority.lastIndexOf("@");
  if (at !== -1) authority = `${REDACTED_URL_CREDENTIAL}${authority.slice(at)}`;
  const host = authority.slice(authority.lastIndexOf("@") + 1);
  const hashIndex = tail.indexOf("#");
  const beforeFragment = hashIndex === -1 ? tail : tail.slice(0, hashIndex);
  const fragment = hashIndex === -1 ? undefined : tail.slice(hashIndex + 1);
  const queryIndex = beforeFragment.indexOf("?");
  const path =
    queryIndex === -1 ? beforeFragment : beforeFragment.slice(0, queryIndex);
  const query =
    queryIndex === -1 ? undefined : beforeFragment.slice(queryIndex + 1);
  return [
    prefix,
    authority,
    redactPath(path, host),
    query === undefined ? "" : `?${redactParameters(query)}`,
    fragment === undefined ? "" : `#${redactParameters(fragment)}`,
    trailing,
  ].join("");
}

/**
 * Replace the credentials in every URL inside `text`, keeping the rest intact.
 *
 * Covers userinfo, credential-named query, fragment and path parameters
 * (including `&amp;`-escaped, `\u0026`-escaped and `;`-separated ones),
 * JWT-shaped values, credential-bearing path segments, and URLs nested inside
 * parameter values, whether percent-encoded once, several times or not at all. Returns `text` itself when nothing matched,
 * and applying it twice gives the same result as applying it once.
 */
export function redactUrlCredentials(text: string): string {
  if (!text.includes(":/") && !text.includes(":\\/")) return text;
  return text.replace(URL_IN_TEXT, redactUrl);
}

/** Whether `text` holds a URL credential that `redactUrlCredentials` would replace. */
export function containsUrlCredentials(text: string): boolean {
  return redactUrlCredentials(text) !== text;
}
