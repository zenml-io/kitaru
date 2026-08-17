import { describe, expect, it, vi } from "vitest";

import {
  bindCredentialProvider,
  createStaticCredentialProvider,
  type RenewableCredentialProvider,
} from "../../src/auth/index.js";
import { KitaruTransport } from "../../src/transport.js";

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("credential providers", () => {
  it("wraps a static credential in a stable, non-renewable identity", async () => {
    const provider = createStaticCredentialProvider("KITKEY_static");

    await expect(
      provider.getCredential(new AbortController().signal),
    ).resolves.toEqual({
      generation: 0,
      identity: "explicit",
      token: "KITKEY_static",
    });
    expect("renewCredential" in provider).toBe(false);
  });

  it("renews once after 401, shares renewal, and stops after a second 401", async () => {
    let generation = 0;
    let releaseRenewal: (() => void) | undefined;
    const renewalStarted = new Promise<void>((resolve) => {
      releaseRenewal = resolve;
    });
    const renew = vi.fn(async () => {
      await renewalStarted;
      generation += 1;
      return { token: "fresh", identity: "stored:device", generation };
    });
    const provider: RenewableCredentialProvider = {
      getCredential: async () => ({
        token: generation === 0 ? "stale" : "fresh",
        identity: "stored:device",
        generation,
      }),
      renewCredential: renew,
    };
    const fetch = vi.fn<typeof globalThis.fetch>(async (_input, init) => {
      const authorization = (init?.headers as Record<string, string>)
        .Authorization;
      return authorization === "Bearer fresh"
        ? jsonResponse({ ok: true })
        : jsonResponse({ detail: "unauthorized" }, 401);
    });
    const transport = new KitaruTransport({
      apiUrl: "https://api.example",
      credentialProvider: provider,
      fetch,
      timeoutMs: 1_000,
    });

    const first = transport.request({ method: "GET", path: "/v1/private" });
    const second = transport.request({ method: "GET", path: "/v1/private" });
    releaseRenewal?.();

    await expect(Promise.all([first, second])).resolves.toEqual([
      { ok: true },
      { ok: true },
    ]);
    expect(renew).toHaveBeenCalledTimes(2);

    const alwaysUnauthorized = new KitaruTransport({
      apiUrl: "https://api.example",
      credentialProvider: provider,
      fetch: vi.fn(async () => jsonResponse({ detail: "no" }, 401)),
      timeoutMs: 1_000,
    });
    await expect(
      alwaysUnauthorized.request({ method: "GET", path: "/v1/private" }),
    ).rejects.toMatchObject({ status: 401 });
  });

  it("does not renew after 403 and keeps one deadline through renewal", async () => {
    const renew = vi.fn(
      (_rejected, signal: AbortSignal): Promise<never> =>
        new Promise((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(signal.reason), {
            once: true,
          });
        }),
    );
    const provider: RenewableCredentialProvider = {
      getCredential: async () => ({
        generation: 0,
        identity: "stored:device",
        token: "stale",
      }),
      renewCredential: renew,
    };
    const forbidden = new KitaruTransport({
      apiUrl: "https://api.example",
      credentialProvider: provider,
      fetch: vi.fn(async () => jsonResponse({ detail: "forbidden" }, 403)),
      timeoutMs: 1_000,
    });
    await expect(
      forbidden.request({ method: "GET", path: "/v1/private" }),
    ).rejects.toMatchObject({ status: 403 });
    expect(renew).not.toHaveBeenCalled();

    const unauthorized = new KitaruTransport({
      apiUrl: "https://api.example",
      credentialProvider: provider,
      fetch: vi.fn(async () => jsonResponse({ detail: "unauthorized" }, 401)),
      timeoutMs: 5,
    });
    await expect(
      unauthorized.request({ method: "GET", path: "/v1/private" }),
    ).rejects.toMatchObject({ kind: "timeout" });
  });

  it("binds a custom callback to its first explicit identity", async () => {
    const callback = vi
      .fn()
      .mockResolvedValueOnce("first")
      .mockResolvedValueOnce("second");
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      jsonResponse({ ok: true }),
    );
    const transport = new KitaruTransport({
      apiUrl: "https://api.example",
      credentialProvider: callback,
      fetch,
      timeoutMs: 1_000,
    });

    await transport.request({ method: "GET", path: "/v1/one" });
    await transport.request({ method: "GET", path: "/v1/two" });

    expect(callback).toHaveBeenCalledOnce();
    expect(fetch.mock.calls.map((call) => call[1]?.headers)).toEqual([
      expect.objectContaining({ Authorization: "Bearer first" }),
      expect.objectContaining({ Authorization: "Bearer first" }),
    ]);
  });

  it("retries a custom credential callback after a transient failure", async () => {
    const callback = vi
      .fn()
      .mockRejectedValueOnce(new Error("secret manager unavailable"))
      .mockResolvedValueOnce("recovered");
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      jsonResponse({ ok: true }),
    );
    const transport = new KitaruTransport({
      apiUrl: "https://api.example",
      credentialProvider: callback,
      fetch,
      timeoutMs: 1_000,
    });

    await expect(
      transport.request({ method: "GET", path: "/v1/first" }),
    ).rejects.toThrow("Credential lookup failed");
    await expect(
      transport.request({ method: "GET", path: "/v1/second" }),
    ).resolves.toEqual({ ok: true });

    expect(callback).toHaveBeenCalledTimes(2);
    expect(fetch).toHaveBeenCalledOnce();
    expect(fetch.mock.calls[0]?.[1]?.headers).toMatchObject({
      Authorization: "Bearer recovered",
    });
  });

  it("isolates callback credential cancellation between concurrent callers", async () => {
    let providerSignal: AbortSignal | undefined;
    let resolveCredential: ((token: string) => void) | undefined;
    const callback = vi.fn(
      (signal: AbortSignal) =>
        new Promise<string>((resolve) => {
          providerSignal = signal;
          resolveCredential = resolve;
        }),
    );
    const provider = bindCredentialProvider(callback);
    const firstController = new AbortController();
    const secondController = new AbortController();

    const first = provider.getCredential(firstController.signal);
    const second = provider.getCredential(secondController.signal);
    await vi.waitFor(() => expect(callback).toHaveBeenCalledOnce());

    const firstCancelled = expect(first).rejects.toThrow("first cancelled");
    firstController.abort(new Error("first cancelled"));
    await firstCancelled;
    expect(providerSignal?.aborted).toBe(false);

    resolveCredential?.("shared");
    await expect(second).resolves.toEqual({
      generation: 0,
      identity: "custom",
      token: "shared",
    });
    expect(callback).toHaveBeenCalledOnce();
  });

  it("abandons a callback credential lookup when its only caller cancels", async () => {
    const providerSignals: AbortSignal[] = [];
    const callback = vi.fn(
      (signal: AbortSignal) =>
        new Promise<string>((resolve, reject) => {
          providerSignals.push(signal);
          if (providerSignals.length === 2) {
            resolve("recovered");
            return;
          }
          signal.addEventListener("abort", () => reject(signal.reason), {
            once: true,
          });
        }),
    );
    const provider = bindCredentialProvider(callback);
    const controller = new AbortController();

    const first = provider.getCredential(controller.signal);
    await vi.waitFor(() => expect(callback).toHaveBeenCalledOnce());
    const firstCancelled = expect(first).rejects.toThrow("request cancelled");
    controller.abort(new Error("request cancelled"));
    await firstCancelled;
    expect(providerSignals[0]?.aborted).toBe(true);

    await expect(
      provider.getCredential(new AbortController().signal),
    ).resolves.toEqual({
      generation: 0,
      identity: "custom",
      token: "recovered",
    });
    expect(callback).toHaveBeenCalledTimes(2);
  });
});
