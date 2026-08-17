import { describe, expect, it, vi } from "vitest";

import { KitaruClient } from "../../src/index.js";

const ID = "018f0000-0000-7000-8000-000000000001";
const metadata = {
  created: "2026-01-01T00:00:00Z",
  id: ID,
  media_type: "text/plain",
  sha256: "a".repeat(64),
  size: 5,
};

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("blobs resource", () => {
  it("uploads bytes with filename and media type", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async (_url, init) => {
      const body = init?.body as FormData;
      const file = body.get("file") as File;
      expect(file.name).toBe("evaluator.py");
      expect(file.type).toBe("text/x-python");
      expect(new Uint8Array(await file.arrayBuffer())).toEqual(
        new TextEncoder().encode("hello"),
      );
      return jsonResponse(metadata, 201);
    });
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(
      client.blobs.upload(new TextEncoder().encode("hello"), {
        filename: "evaluator.py",
        mediaType: "text/x-python",
      }),
    ).resolves.toEqual(metadata);
  });

  it("uploads Blob values and downloads binary content", async () => {
    const bytes = new Uint8Array([0, 1, 2, 255]);
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockImplementationOnce(async (_url, init) => {
        const file = (init?.body as FormData).get("file") as File;
        expect(file.name).toBe("payload.bin");
        expect(file.type).toBe("application/custom");
        return jsonResponse(metadata, 201);
      })
      .mockResolvedValueOnce(new Response(bytes));
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await client.blobs.upload(new Blob([bytes], { type: "ignored/type" }), {
      filename: "payload.bin",
      mediaType: "application/custom",
    });
    await expect(client.blobs.download(ID)).resolves.toEqual(bytes);
  });

  it("gets blob metadata and rejects malformed metadata", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(jsonResponse(metadata))
      .mockResolvedValueOnce(jsonResponse({ ...metadata, size: "five" }));
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(client.blobs.get(ID)).resolves.toEqual(metadata);
    await expect(client.blobs.get(ID)).rejects.toThrow("invalid size");
  });

  it("deletes blob metadata with an empty response", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(
      async () => new Response(null, { status: 204 }),
    );
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(client.blobs.delete(ID)).resolves.toBeUndefined();
    expect(fetch).toHaveBeenCalledWith(
      `https://api.example/api/v1/blobs/${ID}`,
      expect.objectContaining({ method: "DELETE" }),
    );
  });
});
