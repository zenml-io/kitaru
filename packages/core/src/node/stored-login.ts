import { createHash } from "node:crypto";
import { constants } from "node:fs";
import { lstat, open } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";

import type {
  RenewableCredentialProvider,
  ResolvedCredential,
} from "../auth/index.js";
import { KitaruCredentialError } from "../auth/index.js";
import { normalizeApiUrl } from "../transport.js";
import { isRecord, isUuid } from "../validation.js";

const MAX_STORE_BYTES = 1024 * 1024;
const DEVICE_GRANT = "urn:ietf:params:oauth:grant-type:device_code";
const DISABLED_VALUES = new Set(["1", "true", "yes"]);

export type StoredLoginEnvironment = Readonly<
  Record<string, string | undefined>
>;

export interface StoredLoginOptions {
  apiUrl: string;
  configDirectory: string;
  fetch?: typeof globalThis.fetch;
  timeoutMs: number;
}

interface StoredToken {
  accessToken: string;
  expiresAt?: number;
  leewaySeconds: number;
}

interface StoredEntry {
  key: string;
  url: string;
  type: "server" | "control_plane";
  apiKey?: string;
  token?: StoredToken;
  deviceId?: string;
  deviceCode?: string;
  controlPlaneApiUrl?: string;
}

interface StoreSnapshot {
  selected: StoredEntry;
  controlPlane?: StoredEntry;
  identity: string;
}

function fingerprint(...parts: string[]): string {
  return createHash("sha256").update(parts.join("\0")).digest("hex");
}

function fail(message: string): never {
  throw new KitaruCredentialError(`Stored Kitaru login is invalid: ${message}`);
}

function requireString(
  value: unknown,
  field: string,
  optional = false,
): string | undefined {
  if (value === undefined && optional) {
    return undefined;
  }
  if (typeof value !== "string" || value.length === 0) {
    fail(`${field} must be a non-empty string`);
  }
  return value;
}

function normalizeStoreUrl(value: string, field: string): string {
  const stripped = value.replace(/\/+$/, "");
  try {
    return normalizeApiUrl(stripped);
  } catch (error) {
    throw new KitaruCredentialError(
      `Stored Kitaru login is invalid: ${field} is unsafe`,
      {
        cause: error,
      },
    );
  }
}

function parseToken(value: unknown): StoredToken | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (!isRecord(value)) {
    fail("api_token must be an object");
  }
  const accessToken = requireString(
    value.access_token,
    "api_token.access_token",
  ) as string;
  let expiresAt: number | undefined;
  if (value.expires_at !== undefined && value.expires_at !== null) {
    if (typeof value.expires_at !== "string") {
      fail("api_token.expires_at must be an ISO timestamp");
    }
    expiresAt = Date.parse(value.expires_at);
    if (!Number.isFinite(expiresAt)) {
      fail("api_token.expires_at must be an ISO timestamp");
    }
  }
  const leeway = value.leeway_seconds ?? 0;
  if (typeof leeway !== "number" || !Number.isFinite(leeway) || leeway < 0) {
    fail("api_token.leeway_seconds must be a non-negative number");
  }
  return { accessToken, expiresAt, leewaySeconds: leeway };
}

function parseEntry(key: string, value: unknown): StoredEntry {
  if (!isRecord(value)) {
    fail("the selected credential entry must be an object");
  }
  const urlValue = requireString(value.url, "url") as string;
  const normalizedKey = normalizeStoreUrl(key, "credential key");
  const url = normalizeStoreUrl(urlValue, "credential url");
  if (url !== normalizedKey) {
    fail("the selected credential key and embedded URL do not match");
  }
  const type = value.type ?? "server";
  if (type !== "server" && type !== "control_plane") {
    fail("type must be server or control_plane");
  }
  const apiKey = requireString(value.api_key, "api_key", true);
  if (
    apiKey !== undefined &&
    !apiKey.startsWith("KITKEY_") &&
    !apiKey.startsWith("ZENPROKEY_")
  ) {
    fail("api_key has an unsupported prefix");
  }
  const deviceId = requireString(value.device_id, "device_id", true);
  const deviceCode = requireString(value.device_code, "device_code", true);
  if ((deviceId === undefined) !== (deviceCode === undefined)) {
    fail("device_id and device_code must appear together");
  }
  if (deviceId !== undefined && !isUuid(deviceId)) {
    fail("device_id must be a UUID");
  }
  const controlPlaneValue = requireString(
    value.control_plane_api_url,
    "control_plane_api_url",
    true,
  );
  return {
    apiKey,
    controlPlaneApiUrl:
      controlPlaneValue === undefined
        ? undefined
        : normalizeStoreUrl(controlPlaneValue, "control_plane_api_url"),
    deviceCode,
    deviceId,
    key,
    token: parseToken(value.api_token),
    type,
    url,
  };
}

function tokenIsValid(token: StoredToken | undefined): token is StoredToken {
  return (
    token !== undefined &&
    (token.expiresAt === undefined ||
      Date.now() < token.expiresAt - token.leewaySeconds * 1_000)
  );
}

async function awaitWithSignal<T>(
  pending: Promise<T>,
  signal: AbortSignal,
): Promise<T> {
  if (signal.aborted) {
    throw signal.reason;
  }
  return new Promise<T>((resolve, reject) => {
    const abort = () => reject(signal.reason);
    signal.addEventListener("abort", abort, { once: true });
    pending.then(
      (value) => {
        signal.removeEventListener("abort", abort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", abort);
        reject(error);
      },
    );
  });
}

async function readBoundedJson(
  path: string,
  ownerOnly: boolean,
): Promise<unknown> {
  const before = await lstat(path).catch((error: NodeJS.ErrnoException) => {
    if (error.code === "ENOENT") {
      throw new KitaruCredentialError(
        `No stored Kitaru login was found at ${path}`,
      );
    }
    throw error;
  });
  if (before.isSymbolicLink()) {
    throw new KitaruCredentialError(
      `Refusing to read stored Kitaru login symlink at ${path}`,
    );
  }
  if (!before.isFile()) {
    throw new KitaruCredentialError(
      `Stored Kitaru login path is not a regular file: ${path}`,
    );
  }
  const noFollow =
    process.platform === "win32" ? 0 : (constants.O_NOFOLLOW ?? 0);
  const handle = await open(path, constants.O_RDONLY | noFollow);
  try {
    const opened = await handle.stat();
    if (!opened.isFile() || opened.size > MAX_STORE_BYTES) {
      throw new KitaruCredentialError(
        `Stored Kitaru login file is not a bounded regular file: ${path}`,
      );
    }
    if (ownerOnly && process.platform !== "win32") {
      if ((opened.mode & 0o077) !== 0 || opened.uid !== process.getuid?.()) {
        throw new KitaruCredentialError(
          "Stored Kitaru credentials must be owner-only",
        );
      }
    }
    const contents = await handle.readFile({ encoding: "utf8" });
    const after = await lstat(path);
    if (
      after.dev !== opened.dev ||
      after.ino !== opened.ino ||
      after.size !== opened.size ||
      after.mtimeMs !== opened.mtimeMs
    ) {
      throw new KitaruCredentialError(
        "Stored Kitaru login changed while it was being read",
      );
    }
    try {
      return JSON.parse(contents) as unknown;
    } catch (error) {
      throw new KitaruCredentialError(
        `Stored Kitaru login JSON is malformed at ${path}`,
        {
          cause: error,
        },
      );
    }
  } finally {
    await handle.close();
  }
}

function durableIdentity(selected: StoredEntry, cp?: StoredEntry): string {
  if (selected.apiKey?.startsWith("KITKEY_")) {
    return `server-key:${fingerprint(selected.key, selected.apiKey)}`;
  }
  if (selected.deviceId !== undefined && selected.deviceCode !== undefined) {
    return `device:${fingerprint(selected.key, selected.deviceId, selected.deviceCode)}`;
  }
  if (selected.apiKey?.startsWith("ZENPROKEY_")) {
    return `embedded-control-plane-key:${fingerprint(selected.key, selected.apiKey)}`;
  }
  if (selected.controlPlaneApiUrl !== undefined && cp !== undefined) {
    if (cp.apiKey?.startsWith("ZENPROKEY_")) {
      return `control-plane-key:${fingerprint(selected.key, cp.key, cp.apiKey)}`;
    }
    if (cp.token !== undefined) {
      return `control-plane-token:${fingerprint(selected.key, cp.key, cp.token.accessToken)}`;
    }
  }
  if (selected.token !== undefined) {
    return `token:${fingerprint(selected.key, selected.token.accessToken)}`;
  }
  return `unusable:${fingerprint(selected.key)}`;
}

async function loadSnapshot(
  configDirectory: string,
  apiUrl: string,
): Promise<StoreSnapshot> {
  const payload = await readBoundedJson(
    join(configDirectory, "credentials.json"),
    true,
  );
  if (!isRecord(payload)) {
    fail("credentials.json must contain an object");
  }
  const canonicalUrl = normalizeStoreUrl(apiUrl, "server URL");
  const collidingKeys = Object.keys(payload).filter((candidate) => {
    try {
      return normalizeStoreUrl(candidate, "credential key") === canonicalUrl;
    } catch {
      return false;
    }
  });
  if (collidingKeys.length > 1) {
    fail("credentials.json contains colliding canonical server keys");
  }
  const key = collidingKeys[0];
  if (key === undefined) {
    throw new KitaruCredentialError(
      `No stored Kitaru login exists for ${canonicalUrl}; run kitaru login again`,
    );
  }
  const selected = parseEntry(key, payload[key]);
  if (selected.type !== "server") {
    fail("the selected server entry has the wrong type");
  }
  let controlPlane: StoredEntry | undefined;
  if (selected.controlPlaneApiUrl !== undefined) {
    const controlPlaneKeys = Object.keys(payload).filter((candidate) => {
      try {
        return (
          normalizeStoreUrl(candidate, "credential key") ===
          selected.controlPlaneApiUrl
        );
      } catch {
        return false;
      }
    });
    if (controlPlaneKeys.length > 1) {
      fail("credentials.json contains colliding canonical control-plane keys");
    }
    const controlPlaneKey = controlPlaneKeys[0];
    if (controlPlaneKey !== undefined) {
      controlPlane = parseEntry(controlPlaneKey, payload[controlPlaneKey]);
      if (controlPlane.type !== "control_plane") {
        fail("the referenced control-plane entry has the wrong type");
      }
    }
  }
  return {
    controlPlane,
    identity: durableIdentity(selected, controlPlane),
    selected,
  };
}

function form(values: Readonly<Record<string, string>>): URLSearchParams {
  return new URLSearchParams(values);
}

async function exchange(
  fetch: typeof globalThis.fetch,
  url: string,
  body: URLSearchParams,
  signal: AbortSignal,
  bearer?: string,
): Promise<StoredToken> {
  const response = await fetch(url, {
    body,
    headers: {
      Accept: "application/json",
      "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
      ...(bearer === undefined ? {} : { Authorization: `Bearer ${bearer}` }),
    },
    method: "POST",
    redirect: "manual",
    signal,
  });
  if (response.status >= 300 && response.status < 400) {
    await response.body?.cancel().catch(() => undefined);
    throw new KitaruCredentialError(
      "Stored-login credential exchange refused a redirect",
    );
  }
  if (!response.ok) {
    await response.body?.cancel().catch(() => undefined);
    throw new KitaruCredentialError(
      `Stored-login credential exchange failed with HTTP ${response.status}`,
    );
  }
  const value: unknown = await response.json();
  if (
    !isRecord(value) ||
    typeof value.access_token !== "string" ||
    value.access_token.length === 0
  ) {
    throw new KitaruCredentialError(
      "Stored-login credential exchange returned an invalid token",
    );
  }
  if (
    typeof value.expires_in !== "number" ||
    !Number.isFinite(value.expires_in) ||
    value.expires_in <= 0
  ) {
    throw new KitaruCredentialError(
      "Stored-login credential exchange returned an invalid expiry",
    );
  }
  const leewaySeconds = Math.max(30, Math.floor(value.expires_in / 20));
  return {
    accessToken: value.access_token,
    expiresAt: Date.now() + value.expires_in * 1_000,
    leewaySeconds,
  };
}

export class StoredLoginCredentialProvider
  implements RenewableCredentialProvider
{
  readonly #apiUrl: string;
  readonly #configDirectory: string;
  readonly #exchangeTimeoutMs: number;
  readonly #fetch: typeof globalThis.fetch;
  #current?: ResolvedCredential;
  #identity?: string;
  #initial?: Promise<ResolvedCredential>;
  #renewal?: Promise<ResolvedCredential>;

  constructor(options: StoredLoginOptions) {
    this.#apiUrl = normalizeApiUrl(options.apiUrl);
    this.#configDirectory = options.configDirectory;
    if (!Number.isFinite(options.timeoutMs) || options.timeoutMs <= 0) {
      throw new Error("timeoutMs must be a positive finite number");
    }
    this.#exchangeTimeoutMs = Math.ceil(options.timeoutMs);
    this.#fetch = options.fetch ?? globalThis.fetch;
  }

  async getCredential(signal: AbortSignal): Promise<ResolvedCredential> {
    if (this.#current !== undefined) {
      return this.#current;
    }
    this.#initial ??= this.#initialize(
      AbortSignal.timeout(this.#exchangeTimeoutMs),
    ).finally(() => {
      this.#initial = undefined;
    });
    return awaitWithSignal(this.#initial, signal);
  }

  async #initialize(signal: AbortSignal): Promise<ResolvedCredential> {
    const snapshot = await loadSnapshot(this.#configDirectory, this.#apiUrl);
    this.#identity = snapshot.identity;
    const token = await this.#getInitialToken(snapshot, signal);
    this.#current = { generation: 0, identity: snapshot.identity, token };
    return this.#current;
  }

  async renewCredential(
    rejected: ResolvedCredential,
    signal: AbortSignal,
  ): Promise<ResolvedCredential> {
    if (
      this.#current !== undefined &&
      this.#current.generation !== rejected.generation
    ) {
      return this.#current;
    }
    this.#renewal ??= this.#renew(
      rejected,
      AbortSignal.timeout(this.#exchangeTimeoutMs),
    ).finally(() => {
      this.#renewal = undefined;
    });
    return awaitWithSignal(this.#renewal, signal);
  }

  async #getInitialToken(
    snapshot: StoreSnapshot,
    signal: AbortSignal,
  ): Promise<string> {
    if (snapshot.selected.apiKey?.startsWith("KITKEY_")) {
      return snapshot.selected.apiKey;
    }
    if (tokenIsValid(snapshot.selected.token)) {
      return snapshot.selected.token.accessToken;
    }
    return (await this.#exchangeSnapshot(snapshot, signal)).accessToken;
  }

  async #renew(
    rejected: ResolvedCredential,
    signal: AbortSignal,
  ): Promise<ResolvedCredential> {
    const snapshot = await loadSnapshot(this.#configDirectory, this.#apiUrl);
    if (
      snapshot.identity !== this.#identity ||
      rejected.identity !== this.#identity
    ) {
      throw new KitaruCredentialError(
        "Stored Kitaru identity changed; create a new client",
      );
    }
    const token = await this.#exchangeSnapshot(snapshot, signal);
    const next = {
      generation: rejected.generation + 1,
      identity: rejected.identity,
      token: token.accessToken,
    };
    this.#current = next;
    return next;
  }

  async #exchangeSnapshot(
    snapshot: StoreSnapshot,
    signal: AbortSignal,
  ): Promise<StoredToken> {
    const selected = snapshot.selected;
    if (selected.deviceId !== undefined && selected.deviceCode !== undefined) {
      return exchange(
        this.#fetch,
        `${this.#apiUrl}/v1/login`,
        form({
          device_code: selected.deviceCode,
          device_id: selected.deviceId,
          grant_type: DEVICE_GRANT,
        }),
        signal,
      );
    }

    let controlPlaneCredential = selected.apiKey?.startsWith("ZENPROKEY_")
      ? selected.apiKey
      : undefined;
    if (
      controlPlaneCredential === undefined &&
      snapshot.controlPlane !== undefined
    ) {
      const cp = snapshot.controlPlane;
      if (tokenIsValid(cp.token)) {
        controlPlaneCredential = cp.token.accessToken;
      } else if (cp.apiKey?.startsWith("ZENPROKEY_")) {
        controlPlaneCredential = (
          await exchange(
            this.#fetch,
            `${cp.url}/auth/login`,
            form({ grant_type: "zenml_api_key", password: cp.apiKey }),
            signal,
            cp.apiKey,
          )
        ).accessToken;
      }
    }
    if (controlPlaneCredential !== undefined) {
      return exchange(
        this.#fetch,
        `${this.#apiUrl}/v1/login`,
        form({ grant_type: "control-plane" }),
        signal,
        controlPlaneCredential,
      );
    }
    throw new KitaruCredentialError(
      "Stored credential cannot be renewed non-interactively; run kitaru login again",
    );
  }
}

export function resolveConfigDirectory(
  environment: StoredLoginEnvironment,
  homeDirectory = homedir(),
): string {
  if (environment.KITARU_CONFIG_DIR) {
    return environment.KITARU_CONFIG_DIR;
  }
  if (environment.XDG_CONFIG_HOME) {
    return join(environment.XDG_CONFIG_HOME, "kitaru");
  }
  return join(homeDirectory, ".config", "kitaru");
}

export function credentialsCacheDisabled(
  environment: StoredLoginEnvironment,
): boolean {
  return DISABLED_VALUES.has(
    (environment.KITARU_DISABLE_CREDENTIALS_CACHE ?? "").toLowerCase(),
  );
}

export async function readSelectedServerUrl(
  configDirectory: string,
): Promise<string> {
  const payload = await readBoundedJson(
    join(configDirectory, "config.json"),
    false,
  );
  if (
    !isRecord(payload) ||
    typeof payload.server_url !== "string" ||
    payload.server_url.length === 0
  ) {
    throw new KitaruCredentialError(
      "No stored Kitaru server is selected; run kitaru login again",
    );
  }
  return normalizeStoreUrl(payload.server_url, "config server_url");
}
