import { describe, expect, it, vi } from "vitest";

import { KitaruApiError } from "../src/errors.js";
import {
  bytesBody,
  formBody,
  jsonBody,
  KitaruTransport,
  multipartBody,
  normalizeApiUrl,
} from "../src/transport.js";

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("KitaruTransport", () => {
  it("serializes query, JSON, form, and bytes and decodes empty and binary responses", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(jsonResponse({ ok: true }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(new Response(Uint8Array.from([1, 2, 3])));
    const transport = new KitaruTransport({
      apiUrl: "https://api.example",
      fetch,
      timeoutMs: 1_000,
    });

    await expect(
      transport.request<{ ok: boolean }>({
        method: "POST",
        path: "/v1/items",
        query: {
          active: false,
          cursor: "opaque+/= cursor",
          labels: ["one", "two"],
          omitted: undefined,
        },
        body: jsonBody({ name: "item" }),
      }),
    ).resolves.toEqual({ ok: true });
    await expect(
      transport.request<void>({
        method: "POST",
        path: "/v1/token",
        body: formBody({ grant_type: "device_code", scope: ["read", "write"] }),
        responseType: "empty",
      }),
    ).resolves.toBeUndefined();
    await expect(
      transport.request<Uint8Array>({
        method: "POST",
        path: "/v1/blob",
        body: bytesBody(Uint8Array.from([4, 5, 6])),
        responseType: "bytes",
      }),
    ).resolves.toEqual(Uint8Array.from([1, 2, 3]));

    const [jsonUrl, jsonInit] = fetch.mock.calls[0] ?? [];
    expect(jsonUrl).toBe(
      "https://api.example/v1/items?active=false&cursor=opaque%2B%2F%3D+cursor&labels=one&labels=two",
    );
    expect(jsonInit?.body).toBe('{"name":"item"}');
    expect(jsonInit?.headers).toMatchObject({
      "Content-Type": "application/json",
    });

    const formInit = fetch.mock.calls[1]?.[1];
    expect(String(formInit?.body)).toBe(
      "grant_type=device_code&scope=read&scope=write",
    );
    expect(formInit?.headers).toMatchObject({
      "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
    });
    expect(
      new Uint8Array(fetch.mock.calls[2]?.[1]?.body as ArrayBuffer),
    ).toEqual(Uint8Array.from([4, 5, 6]));
  });

  it("rebuilds form and buffered multipart bodies for an allowed retry", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockRejectedValueOnce(new TypeError("network failed"))
      .mockResolvedValueOnce(jsonResponse({ ok: true }))
      .mockResolvedValueOnce(jsonResponse({ detail: "unavailable" }, 503))
      .mockResolvedValueOnce(jsonResponse({ ok: true }));
    const transport = new KitaruTransport({
      apiUrl: "https://api.example",
      fetch,
      timeoutMs: 1_000,
    });
    const retry = {
      attempts: 2,
      retryTransportErrors: true,
      statuses: new Set([503]),
    };

    await transport.request({
      method: "POST",
      path: "/v1/form",
      body: formBody({ value: "same" }),
      retry,
    });
    await transport.request({
      method: "POST",
      path: "/v1/upload",
      body: multipartBody([
        { name: "metadata", value: "same" },
        {
          filename: "payload.bin",
          name: "file",
          value: Uint8Array.from([1, 2, 3]),
        },
      ]),
      retry,
    });

    const firstForm = fetch.mock.calls[0]?.[1]?.body;
    const secondForm = fetch.mock.calls[1]?.[1]?.body;
    expect(firstForm).toBeInstanceOf(URLSearchParams);
    expect(secondForm).toBeInstanceOf(URLSearchParams);
    expect(firstForm).not.toBe(secondForm);
    expect(String(firstForm)).toBe(String(secondForm));

    const firstMultipart = fetch.mock.calls[2]?.[1]?.body;
    const secondMultipart = fetch.mock.calls[3]?.[1]?.body;
    expect(firstMultipart).toBeInstanceOf(FormData);
    expect(secondMultipart).toBeInstanceOf(FormData);
    expect(firstMultipart).not.toBe(secondMultipart);
    expect((firstMultipart as FormData).get("metadata")).toBe("same");
    expect((firstMultipart as FormData).get("file")).toBeInstanceOf(Blob);
    expect((secondMultipart as FormData).get("metadata")).toBe("same");
    expect((secondMultipart as FormData).get("file")).toBeInstanceOf(Blob);
  });

  it("gives each retry attempt a fresh timeout", async () => {
    vi.useFakeTimers();
    try {
      const fetch = vi.fn<typeof globalThis.fetch>(
        (_input, init): Promise<Response> =>
          new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
              resolve(
                fetch.mock.calls.length === 1
                  ? jsonResponse({ detail: "unavailable" }, 503)
                  : jsonResponse({ ok: true }),
              );
            }, 80);
            init?.signal?.addEventListener(
              "abort",
              () => {
                clearTimeout(timer);
                reject(new DOMException("Aborted", "AbortError"));
              },
              { once: true },
            );
          }),
      );
      const transport = new KitaruTransport({
        apiUrl: "https://api.example",
        fetch,
        timeoutMs: 100,
      });

      const result = transport.request<{ ok: boolean }>({
        method: "GET",
        path: "/v1/items",
        retry: { attempts: 2, statuses: new Set([503]) },
      });

      await vi.advanceTimersByTimeAsync(80);
      await vi.advanceTimersByTimeAsync(80);
      await expect(result).resolves.toEqual({ ok: true });
      expect(fetch).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it("does not retry a mutation unless the endpoint opts in", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      jsonResponse({ detail: "unavailable" }, 503),
    );
    const transport = new KitaruTransport({
      apiUrl: "https://api.example",
      fetch,
      timeoutMs: 1_000,
    });

    await expect(
      transport.request({
        method: "POST",
        path: "/v1/items",
        body: jsonBody({ name: "item" }),
      }),
    ).rejects.toMatchObject({ status: 503 });
    expect(fetch).toHaveBeenCalledOnce();
  });

  it("distinguishes caller cancellation from the shared request deadline", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(
      (_input, init): Promise<Response> =>
        new Promise((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => {
            reject(new DOMException("Aborted", "AbortError"));
          });
        }),
    );
    const transport = new KitaruTransport({
      apiUrl: "https://api.example",
      fetch,
      timeoutMs: 5,
    });
    const caller = new AbortController();
    const canceled = transport.request({
      method: "GET",
      path: "/v1/canceled",
      signal: caller.signal,
    });
    caller.abort();

    await expect(canceled).rejects.toMatchObject({
      kind: "canceled",
      message: expect.stringContaining("canceled by caller"),
    });
    await expect(
      transport.request({ method: "GET", path: "/v1/timeout" }),
    ).rejects.toMatchObject({
      kind: "timeout",
      message: expect.stringContaining("timed out after 5ms"),
    });
  });

  it("redacts credentials and validation input from structured errors", async () => {
    const secret = "super-secret-token";
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      jsonResponse(
        {
          detail: [
            {
              input: secret,
              loc: ["body", "token"],
              msg: `Invalid value ${secret}`,
            },
          ],
        },
        422,
      ),
    );
    const transport = new KitaruTransport({
      apiUrl: "https://api.example",
      credentialProvider: async () => secret,
      fetch,
      timeoutMs: 1_000,
    });

    const error = await transport
      .request({ method: "GET", path: "/v1/private" })
      .catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(KitaruApiError);
    expect(error).toMatchObject({
      kind: "api",
      method: "GET",
      path: "/v1/private",
      status: 422,
    });
    expect((error as Error).message).toContain("body.token: Invalid value");
    expect((error as Error).message).not.toContain(secret);
    expect(JSON.stringify(error)).not.toContain(secret);
  });

  it.each([
    "https://api.example/v1/other",
    "https://other.example/v1/private",
  ])("rejects an authenticated redirect to %s without following it", async (target) => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      Response.redirect(target, 307),
    );
    const transport = new KitaruTransport({
      apiUrl: "https://api.example",
      credentialProvider: async () => "secret",
      fetch,
      timeoutMs: 1_000,
    });

    await expect(
      transport.request({ method: "GET", path: "/v1/private" }),
    ).rejects.toMatchObject({ kind: "redirect", status: 307 });
    expect(fetch).toHaveBeenCalledOnce();
    expect(fetch.mock.calls[0]?.[1]?.redirect).toBe("manual");
  });

  it("rejects non-loopback HTTP before reading credentials", async () => {
    const credentialProvider = vi.fn(async () => "secret");

    expect(
      () =>
        new KitaruTransport({
          apiUrl: "http://api.example",
          credentialProvider,
          fetch: vi.fn(),
          timeoutMs: 1_000,
        }),
    ).toThrow("HTTPS");
    expect(credentialProvider).not.toHaveBeenCalled();
  });

  it("canonicalizes valid host spelling while rejecting unsafe paths", () => {
    expect(normalizeApiUrl("https://EXAMPLE.com/base/")).toBe(
      "https://example.com/base",
    );
    expect(normalizeApiUrl("https://example.com:443")).toBe(
      "https://example.com",
    );
    expect(() => normalizeApiUrl("https://example.com/a/../private")).toThrow(
      "unsafe path normalization",
    );
    expect(() => normalizeApiUrl("https://example.com/%2e%2e/private")).toThrow(
      "unsafe path normalization",
    );
  });
});
