import {
  chmod,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  createKitaruClient,
  StoredLoginCredentialProvider,
} from "../../src/node/index.js";

const roots: string[] = [];

async function writeStore(
  serverUrl: string,
  entry: Record<string, unknown>,
): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "kitaru-auth-test-"));
  roots.push(root);
  await mkdir(root, { recursive: true });
  await writeFile(
    join(root, "config.json"),
    JSON.stringify({ server_url: serverUrl }),
  );
  await writeFile(
    join(root, "credentials.json"),
    JSON.stringify({
      [serverUrl.replace(/\/+$/, "")]: { url: serverUrl, ...entry },
    }),
  );
  await chmod(join(root, "credentials.json"), 0o600);
  return root;
}

afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(
    roots.splice(0).map(async (root) => {
      const { rm } = await import("node:fs/promises");
      await rm(root, { force: true, recursive: true });
    }),
  );
});

describe("stored CLI login", () => {
  it.each([
    "https://EXAMPLE.com",
    "https://example.com:443",
  ])("reuses a CLI login whose URL canonicalizes from %s", async (serverUrl) => {
    const root = await writeStore(serverUrl, {
      api_key: "KITKEY_stored",
      type: "server",
    });
    const fetch = vi.fn<typeof globalThis.fetch>(
      async () =>
        new Response(JSON.stringify({ detail: "missing" }), { status: 404 }),
    );

    const client = await createKitaruClient({
      environment: { KITARU_CONFIG_DIR: root },
      fetch,
    });
    await expect(client.getReplay("missing")).rejects.toMatchObject({
      status: 404,
    });

    expect(fetch.mock.calls[0]?.[0]).toBe(
      "https://example.com/v1/replays/missing",
    );
    expect(fetch.mock.calls[0]?.[1]?.headers).toMatchObject({
      Authorization: "Bearer KITKEY_stored",
    });
  });

  it("uses a selected cached token without changing the store", async () => {
    const serverUrl = "https://api.example/base";
    const root = await writeStore(serverUrl, {
      api_token: { access_token: "cached", leeway_seconds: 0 },
      type: "server",
    });
    const path = join(root, "credentials.json");
    const before = await readFile(path);
    const beforeStat = await lstat(path);
    const fetch = vi.fn(
      async (_input: URL | RequestInfo, init?: RequestInit) => {
        expect(init?.headers).toMatchObject({ Authorization: "Bearer cached" });
        return new Response(JSON.stringify({ detail: "missing" }), {
          status: 404,
        });
      },
    );

    const client = await createKitaruClient({
      environment: { KITARU_CONFIG_DIR: root },
      fetch,
    });
    await expect(client.getReplay("missing")).rejects.toMatchObject({
      status: 404,
    });

    expect(await readFile(path)).toEqual(before);
    expect((await lstat(path)).mtimeMs).toBe(beforeStat.mtimeMs);
  });

  it("honors explicit and environment precedence without reading stored credentials", async () => {
    const root = await writeStore("https://stored.example", {
      api_key: "KITKEY_stored",
      type: "server",
    });
    await chmod(join(root, "credentials.json"), 0o000);
    const fetch = vi.fn<typeof globalThis.fetch>(
      async () =>
        new Response(JSON.stringify({ detail: "missing" }), { status: 404 }),
    );

    const client = await createKitaruClient({
      apiKey: "explicit",
      apiUrl: "https://explicit.example",
      environment: {
        KITARU_API_KEY: "legacy",
        KITARU_API_TOKEN: "environment",
        KITARU_API_URL: "https://environment.example",
        KITARU_CONFIG_DIR: root,
      },
      fetch,
    });
    await expect(client.getReplay("missing")).rejects.toMatchObject({
      status: 404,
    });
    expect(fetch.mock.calls[0]?.[0]).toBe(
      "https://explicit.example/v1/replays/missing",
    );
    expect(fetch.mock.calls[0]?.[1]?.headers).toMatchObject({
      Authorization: "Bearer explicit",
    });
  });

  it("never uses a stored credential for a different server", async () => {
    const root = await writeStore("https://first.example", {
      api_key: "KITKEY_first",
      type: "server",
    });
    const fetch = vi.fn<typeof globalThis.fetch>();
    const client = await createKitaruClient({
      apiUrl: "https://second.example",
      environment: { KITARU_CONFIG_DIR: root },
      fetch,
    });

    await expect(client.getReplay("missing")).rejects.toThrow(
      "No stored Kitaru login exists for https://second.example",
    );
    expect(fetch).not.toHaveBeenCalled();
  });

  it("ignores canonical-key collisions for unrelated servers", async () => {
    const serverUrl = "https://selected.example";
    const root = await writeStore(serverUrl, {
      api_key: "KITKEY_selected",
      type: "server",
    });
    const path = join(root, "credentials.json");
    const payload = JSON.parse(await readFile(path, "utf8")) as Record<
      string,
      unknown
    >;
    payload["https://other.example"] = {
      api_key: "KITKEY_other_one",
      type: "server",
      url: "https://other.example",
    };
    payload["https://OTHER.example:443"] = {
      api_key: "KITKEY_other_two",
      type: "server",
      url: "https://OTHER.example:443",
    };
    await writeFile(path, JSON.stringify(payload));
    await chmod(path, 0o600);
    const fetch = vi.fn<typeof globalThis.fetch>(
      async () =>
        new Response(JSON.stringify({ detail: "missing" }), { status: 404 }),
    );

    const client = await createKitaruClient({
      environment: { KITARU_CONFIG_DIR: root },
      fetch,
    });
    await expect(client.getReplay("missing")).rejects.toMatchObject({
      status: 404,
    });
    expect(fetch.mock.calls[0]?.[1]?.headers).toMatchObject({
      Authorization: "Bearer KITKEY_selected",
    });
  });

  it("uses KITKEY directly and exchanges an expired device token read-only", async () => {
    const apiKeyRoot = await writeStore("https://key.example", {
      api_key: "KITKEY_direct",
      type: "server",
    });
    const apiKeyFetch = vi.fn<typeof globalThis.fetch>(
      async () =>
        new Response(JSON.stringify({ detail: "missing" }), { status: 404 }),
    );
    const apiKeyClient = await createKitaruClient({
      environment: { KITARU_CONFIG_DIR: apiKeyRoot },
      fetch: apiKeyFetch,
    });
    await expect(apiKeyClient.getReplay("missing")).rejects.toMatchObject({
      status: 404,
    });
    expect(apiKeyFetch.mock.calls[0]?.[1]?.headers).toMatchObject({
      Authorization: "Bearer KITKEY_direct",
    });

    const deviceRoot = await writeStore("https://device.example", {
      api_token: {
        access_token: "expired",
        expires_at: "2000-01-01T00:00:00Z",
        leeway_seconds: 30,
      },
      device_code: "device-secret",
      device_id: "018f0000-0000-7000-8000-000000000001",
      type: "server",
    });
    const before = await readFile(join(deviceRoot, "credentials.json"));
    const deviceFetch = vi.fn(
      async (input: URL | RequestInfo, init?: RequestInit) => {
        if (String(input).endsWith("/v1/login")) {
          expect(init?.redirect).toBe("manual");
          expect(String(init?.body)).toContain("device_code=device-secret");
          return new Response(
            JSON.stringify({ access_token: "renewed", expires_in: 3600 }),
          );
        }
        expect(init?.headers).toMatchObject({
          Authorization: "Bearer renewed",
        });
        return new Response(JSON.stringify({ detail: "missing" }), {
          status: 404,
        });
      },
    );
    const deviceClient = await createKitaruClient({
      environment: { KITARU_CONFIG_DIR: deviceRoot },
      fetch: deviceFetch,
    });
    await expect(deviceClient.getReplay("missing")).rejects.toMatchObject({
      status: 404,
    });
    expect(await readFile(join(deviceRoot, "credentials.json"))).toEqual(
      before,
    );
  });

  it("rejects token-only expiry, disabled cache, insecure files, and final symlinks", async () => {
    const expiredRoot = await writeStore("https://api.example", {
      api_token: {
        access_token: "expired",
        expires_at: "2000-01-01T00:00:00Z",
      },
      type: "server",
    });
    const expiredClient = await createKitaruClient({
      environment: { KITARU_CONFIG_DIR: expiredRoot },
    });
    await expect(expiredClient.getReplay("missing")).rejects.toThrow(
      "kitaru login again",
    );
    await expect(
      createKitaruClient({
        environment: {
          KITARU_CONFIG_DIR: expiredRoot,
          KITARU_DISABLE_CREDENTIALS_CACHE: "yes",
        },
      }),
    ).rejects.toThrow("disabled");

    await chmod(join(expiredRoot, "credentials.json"), 0o644);
    const insecureClient = await createKitaruClient({
      environment: { KITARU_CONFIG_DIR: expiredRoot },
    });
    await expect(insecureClient.getReplay("missing")).rejects.toThrow(
      "owner-only",
    );

    const linkRoot = await mkdtemp(join(tmpdir(), "kitaru-auth-link-"));
    roots.push(linkRoot);
    await writeFile(
      join(linkRoot, "config.json"),
      JSON.stringify({ server_url: "https://api.example" }),
    );
    await symlink(
      join(expiredRoot, "credentials.json"),
      join(linkRoot, "credentials.json"),
    );
    const symlinkClient = await createKitaruClient({
      environment: { KITARU_CONFIG_DIR: linkRoot },
    });
    await expect(symlinkClient.getReplay("missing")).rejects.toThrow("symlink");
  });

  it("rejects malformed device identities and mismatched stored URLs", async () => {
    const malformedDeviceRoot = await writeStore("https://device.example", {
      device_code: "code",
      device_id: "not-a-uuid",
      type: "server",
    });
    const malformedDeviceClient = await createKitaruClient({
      environment: { KITARU_CONFIG_DIR: malformedDeviceRoot },
    });
    await expect(malformedDeviceClient.getReplay("missing")).rejects.toThrow(
      "device_id must be a UUID",
    );

    const mismatchedRoot = await writeStore("https://stored.example", {
      api_key: "KITKEY_stored",
      type: "server",
      url: "https://other.example",
    });
    const mismatchedClient = await createKitaruClient({
      environment: { KITARU_CONFIG_DIR: mismatchedRoot },
    });
    await expect(mismatchedClient.getReplay("missing")).rejects.toThrow(
      "key and embedded URL do not match",
    );
  });

  it("renews control-plane credentials once for concurrent 401 responses", async () => {
    const serverUrl = "https://server.example";
    const controlPlaneUrl = "https://cp.example";
    const root = await writeStore(serverUrl, {
      api_token: { access_token: "stale" },
      control_plane_api_url: controlPlaneUrl,
      type: "server",
    });
    const path = join(root, "credentials.json");
    const payload = JSON.parse(await readFile(path, "utf8")) as Record<
      string,
      unknown
    >;
    payload[controlPlaneUrl] = {
      api_key: "ZENPROKEY_durable",
      api_token: {
        access_token: "expired-cp",
        expires_at: "2000-01-01T00:00:00Z",
      },
      type: "control_plane",
      url: controlPlaneUrl,
    };
    await writeFile(path, JSON.stringify(payload));
    await chmod(path, 0o600);

    let cpLogins = 0;
    let serverLogins = 0;
    const fetch = vi.fn<typeof globalThis.fetch>(async (input, init) => {
      const url = String(input);
      const authorization = (init?.headers as Record<string, string>)
        .Authorization;
      if (url === `${controlPlaneUrl}/auth/login`) {
        cpLogins += 1;
        expect(authorization).toBe("Bearer ZENPROKEY_durable");
        return new Response(
          JSON.stringify({ access_token: "cp-fresh", expires_in: 3600 }),
        );
      }
      if (url === `${serverUrl}/v1/login`) {
        serverLogins += 1;
        expect(authorization).toBe("Bearer cp-fresh");
        return new Response(
          JSON.stringify({ access_token: "server-fresh", expires_in: 3600 }),
        );
      }
      if (authorization === "Bearer stale") {
        return new Response(JSON.stringify({ detail: "unauthorized" }), {
          status: 401,
        });
      }
      return new Response(JSON.stringify({ id: "missing" }), { status: 200 });
    });
    const client = await createKitaruClient({
      environment: { KITARU_CONFIG_DIR: root },
      fetch,
    });

    await Promise.allSettled([
      client.getReplay("one"),
      client.getReplay("two"),
    ]);
    expect(cpLogins).toBe(1);
    expect(serverLogins).toBe(1);
  });

  it("aborts renewal if the durable identity changes and rejects exchange redirects", async () => {
    const serverUrl = "https://device.example";
    const root = await writeStore(serverUrl, {
      api_token: { access_token: "stale" },
      device_code: "first-code",
      device_id: "018f0000-0000-7000-8000-000000000001",
      type: "server",
    });
    const path = join(root, "credentials.json");
    const fetch = vi.fn<typeof globalThis.fetch>(async (input) => {
      if (!String(input).endsWith("/v1/login")) {
        const payload = JSON.parse(await readFile(path, "utf8")) as Record<
          string,
          Record<string, unknown>
        >;
        const entry = payload[serverUrl];
        if (entry !== undefined) {
          entry.device_code = "second-code";
        }
        await writeFile(path, JSON.stringify(payload));
        await chmod(path, 0o600);
        return new Response(JSON.stringify({ detail: "unauthorized" }), {
          status: 401,
        });
      }
      throw new Error("exchange must not run");
    });
    const client = await createKitaruClient({
      environment: { KITARU_CONFIG_DIR: root },
      fetch,
    });
    await expect(client.getReplay("missing")).rejects.toThrow(
      "identity changed",
    );

    const redirectRoot = await writeStore("https://redirect.example", {
      api_token: {
        access_token: "expired",
        expires_at: "2000-01-01T00:00:00Z",
      },
      device_code: "code",
      device_id: "018f0000-0000-7000-8000-000000000001",
      type: "server",
    });
    const redirectFetch = vi.fn<typeof globalThis.fetch>(async () =>
      Response.redirect("https://other.example/login", 307),
    );
    const redirectClient = await createKitaruClient({
      environment: { KITARU_CONFIG_DIR: redirectRoot },
      fetch: redirectFetch,
    });
    await expect(redirectClient.getReplay("missing")).rejects.toThrow(
      "redirect",
    );
    expect(redirectFetch).toHaveBeenCalledOnce();
    expect(redirectFetch.mock.calls[0]?.[1]?.redirect).toBe("manual");
  });

  it("keeps one shared exchange alive when one waiter aborts", async () => {
    const serverUrl = "https://device.example";
    const root = await writeStore(serverUrl, {
      api_token: {
        access_token: "expired",
        expires_at: "2000-01-01T00:00:00Z",
      },
      device_code: "code",
      device_id: "018f0000-0000-7000-8000-000000000001",
      type: "server",
    });
    let finishExchange: ((response: Response) => void) | undefined;
    const fetch = vi.fn<typeof globalThis.fetch>(
      async () =>
        new Promise<Response>((resolve) => {
          finishExchange = resolve;
        }),
    );
    const provider = new StoredLoginCredentialProvider({
      apiUrl: serverUrl,
      configDirectory: root,
      fetch,
      timeoutMs: 1_000,
    });
    const firstController = new AbortController();
    const first = provider.getCredential(firstController.signal);
    const second = provider.getCredential(new AbortController().signal);
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledOnce());
    firstController.abort();
    finishExchange?.(
      new Response(JSON.stringify({ access_token: "fresh", expires_in: 3600 })),
    );

    await expect(first).rejects.toBeInstanceOf(DOMException);
    await expect(second).resolves.toMatchObject({ token: "fresh" });
    expect(fetch).toHaveBeenCalledOnce();
  });

  it("times out a stuck shared exchange and retries after recovery", async () => {
    const serverUrl = "https://device.example";
    const root = await writeStore(serverUrl, {
      api_token: {
        access_token: "expired",
        expires_at: "2000-01-01T00:00:00Z",
      },
      device_code: "code",
      device_id: "018f0000-0000-7000-8000-000000000001",
      type: "server",
    });
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockImplementationOnce(
        (_input, init): Promise<Response> =>
          new Promise((_resolve, reject) => {
            const signal = init?.signal;
            if (signal?.aborted) {
              reject(signal.reason);
              return;
            }
            signal?.addEventListener("abort", () => reject(signal.reason), {
              once: true,
            });
          }),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({ access_token: "recovered", expires_in: 3600 }),
        ),
      );
    const provider = new StoredLoginCredentialProvider({
      apiUrl: serverUrl,
      configDirectory: root,
      fetch,
      timeoutMs: 5,
    });

    await expect(
      provider.getCredential(new AbortController().signal),
    ).rejects.toBeInstanceOf(DOMException);
    await expect(
      provider.getCredential(new AbortController().signal),
    ).resolves.toMatchObject({ token: "recovered" });
    expect(fetch).toHaveBeenCalledTimes(2);
  });
});
