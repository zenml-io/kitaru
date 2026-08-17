import { type KitaruTransport, multipartBody } from "../transport.js";
import type { BlobResponse } from "../types.js";
import { validateBlob } from "./internal.js";
import type { ResourceRequestOptions } from "./pagination.js";

export interface BlobUploadOptions extends ResourceRequestOptions {
  filename?: string;
  mediaType?: string;
}

export class BlobsResource {
  readonly #transport: KitaruTransport;

  constructor(transport: KitaruTransport) {
    this.#transport = transport;
  }

  async upload(
    content: Blob | ArrayBuffer | Uint8Array,
    options: BlobUploadOptions = {},
  ): Promise<BlobResponse> {
    const mediaType =
      options.mediaType ??
      (content instanceof Blob && content.type
        ? content.type
        : "application/octet-stream");
    return this.#transport.request({
      method: "POST",
      path: "/api/v1/blobs",
      body: multipartBody([
        {
          name: "file",
          value: content,
          filename: options.filename ?? "blob",
          contentType: mediaType,
        },
      ]),
      signal: options.signal,
      validate: validateBlob,
    });
  }

  async get(
    blobId: string,
    options: ResourceRequestOptions = {},
  ): Promise<BlobResponse> {
    return this.#transport.request({
      method: "GET",
      path: `/api/v1/blobs/${encodeURIComponent(blobId)}`,
      signal: options.signal,
      validate: validateBlob,
    });
  }

  async download(
    blobId: string,
    options: ResourceRequestOptions = {},
  ): Promise<Uint8Array> {
    return this.#transport.request({
      method: "GET",
      path: `/api/v1/blobs/${encodeURIComponent(blobId)}/content`,
      responseType: "bytes",
      signal: options.signal,
    });
  }

  async delete(
    blobId: string,
    options: ResourceRequestOptions = {},
  ): Promise<void> {
    return this.#transport.request({
      method: "DELETE",
      path: `/api/v1/blobs/${encodeURIComponent(blobId)}`,
      responseType: "empty",
      signal: options.signal,
    });
  }
}
