import type { KitaruTransport } from "../transport.js";
import type { ServerInfoResponse } from "../types.js";
import { validateInfo } from "./internal.js";
import type { ResourceRequestOptions } from "./pagination.js";

export class InfoResource {
  readonly #transport: KitaruTransport;

  constructor(transport: KitaruTransport) {
    this.#transport = transport;
  }

  async get(options: ResourceRequestOptions = {}): Promise<ServerInfoResponse> {
    return this.#transport.request({
      authenticate: false,
      method: "GET",
      path: "/v1/info",
      signal: options.signal,
      validate: validateInfo,
    });
  }
}
