import { describe, expect, it } from "vitest";
import {
  containsUrlCredentials,
  redactUrlCredentials,
  strictMastraReplayValue,
} from "../../src/adapter/index.js";

const encodeSegment = (text: string) => Buffer.from(text).toString("base64url");
const JWT_VALUE = [
  encodeSegment('{"alg":"HS256","typ":"JWT"}'),
  encodeSegment('{"sub":"test-user"}'),
  encodeSegment("test-signature"),
].join(".");

describe("redactUrlCredentials", () => {
  it.each([
    [
      "a download-token URL",
      "See https://firebasestorage.googleapis.com/v0/b/bucket/o/doc.pdf?alt=media&token=TOKEN_SECRET now",
      "See https://firebasestorage.googleapis.com/v0/b/bucket/o/doc.pdf?alt=media&token=REDACTED now",
    ],
    [
      "a fragment token",
      "https://app.example.com/cb#access_token=FRAG_SECRET&token_type=bearer",
      "https://app.example.com/cb#access_token=REDACTED&token_type=bearer",
    ],
    [
      "userinfo on any scheme",
      "postgres://admin:PG_SECRET@db.internal:5432/app and https://user:PW_SECRET@host.example/a",
      "postgres://REDACTED@db.internal:5432/app and https://REDACTED@host.example/a",
    ],
    [
      "a key parameter",
      "https://maps.googleapis.com/maps/api/geocode/json?address=x&key=GKEY_SECRET",
      "https://maps.googleapis.com/maps/api/geocode/json?address=x&key=REDACTED",
    ],
    [
      "compound credential names",
      "https://api.example.com/a?client_secret=CS_SECRET&password=PW_SECRET&auth_token=AT_SECRET&X-Api-Key=XAK_SECRET&private_token=PT_SECRET",
      "https://api.example.com/a?client_secret=REDACTED&password=REDACTED&auth_token=REDACTED&X-Api-Key=REDACTED&private_token=REDACTED",
    ],
    [
      "signed storage parameters",
      "https://bucket.s3.amazonaws.com/r.pdf?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=TEST_CREDENTIAL_SECRET&X-Amz-Signature=SIG_SECRET",
      "https://bucket.s3.amazonaws.com/r.pdf?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=REDACTED&X-Amz-Signature=REDACTED",
    ],
    [
      "HTML-escaped separators",
      "https://files.example.com/a.pdf?alt=media&amp;token=AMP_SECRET",
      "https://files.example.com/a.pdf?alt=media&amp;token=REDACTED",
    ],
    [
      "semicolon separators and path parameters",
      "https://files.example.com/a;jsessionid=SESSION_SECRET?x=1;token=SEMI_SECRET",
      "https://files.example.com/a;jsessionid=REDACTED?x=1;token=REDACTED",
    ],
    [
      "a percent-encoded nested URL",
      "https://viewer.example.com/view?url=https%3A%2F%2Fbucket.s3.amazonaws.com%2Fr.pdf%3FX-Amz-Signature%3DNESTED_SECRET",
      `https://viewer.example.com/view?url=${encodeURIComponent("https://bucket.s3.amazonaws.com/r.pdf?X-Amz-Signature=REDACTED")}`,
    ],
    [
      "a JSON-escaped URL",
      String.raw`{"u":"https:\/\/files.example.com\/a.pdf?token=ESCAPED_SECRET"}`,
      String.raw`{"u":"https:\/\/files.example.com\/a.pdf?token=REDACTED"}`,
    ],
    [
      "secrets carried in the path",
      `https://hooks.slack.com/services/T000/B000/SLACK_SECRET https://api.telegram.org/bot123456:ABCDEFGHIJKLMNOPQRSTUVWXYZ_SECRET/getMe https://discord.com/api/webhooks/42/DISCORD_SECRET https://api.example.com/token/PATH_SECRET https://api.example.com/v/${JWT_VALUE}`,
      "https://hooks.slack.com/services/T000/B000/REDACTED https://api.telegram.org/REDACTED/getMe https://discord.com/api/webhooks/42/REDACTED https://api.example.com/token/REDACTED https://api.example.com/v/REDACTED",
    ],
    [
      "a JWT under any parameter name",
      `https://app.example.com/?state=${JWT_VALUE}`,
      "https://app.example.com/?state=REDACTED",
    ],
  ])("redacts %s", (_, text, expected) => {
    const redacted = redactUrlCredentials(text);
    expect(redacted).toBe(expected);
    expect(redacted).not.toMatch(/_SECRET/);
    expect(redactUrlCredentials(redacted)).toBe(redacted);
    expect(containsUrlCredentials(text)).toBe(true);
    expect(containsUrlCredentials(redacted)).toBe(false);
  });

  it.each([
    "https://api.example.com/items?page=2&per_page=50&sort=desc&cursor=abc123",
    "https://www.googleapis.com/drive/v3/files?pageToken=CAoQAA&next_token=n2",
    "https://example.com/search?q=token&lang=en#section-2",
    "https://files.example.com/Q3%20Report.pdf and kitaru-file://sha256/abc",
    "plain text with no links, a ratio 3:2 and a time 10:30",
  ])("leaves harmless URLs unchanged: %s", (text) => {
    expect(redactUrlCredentials(text)).toBe(text);
    expect(containsUrlCredentials(text)).toBe(false);
  });

  it("keeps punctuation that follows a URL in prose", () => {
    expect(
      redactUrlCredentials("Open (https://x.example/a?sig=SIG_SECRET)."),
    ).toBe("Open (https://x.example/a?sig=REDACTED).");
  });

  it("makes the strict replay check agree with redaction", () => {
    const text = "https://files.example.com/a.pdf#access_token=SECRET";
    expect(() => strictMastraReplayValue({ text })).toThrow(/URL credentials/);
    expect(
      strictMastraReplayValue({ text: redactUrlCredentials(text) }),
    ).toEqual({
      text: "https://files.example.com/a.pdf#access_token=REDACTED",
    });
  });
});
