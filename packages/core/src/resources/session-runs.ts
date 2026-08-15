import { jsonBody, type KitaruTransport } from "../transport.js";
import type { JobResponse, SessionRunCreateRequest } from "../types.js";
import { validateJob } from "./internal.js";
import type { ResourceRequestOptions } from "./pagination.js";

export class SessionRunsResource {
  readonly #transport: KitaruTransport;

  constructor(transport: KitaruTransport) {
    this.#transport = transport;
  }

  async create(
    request: SessionRunCreateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<JobResponse> {
    return this.#transport.request({
      method: "POST",
      path: "/v1/session-runs",
      body: jsonBody(request),
      signal: options.signal,
      validate: validateJob,
    });
  }
}
