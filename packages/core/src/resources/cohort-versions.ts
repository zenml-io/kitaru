import { jsonBody, type KitaruTransport } from "../transport.js";
import type {
  CohortVersionResponse,
  CohortVersionUpdateRequest,
} from "../types.js";
import { validateCohortVersion } from "./internal.js";
import type { ResourceRequestOptions } from "./pagination.js";

export class CohortVersionsResource {
  readonly #transport: KitaruTransport;

  constructor(transport: KitaruTransport) {
    this.#transport = transport;
  }

  async get(
    cohortVersionId: string,
    options: ResourceRequestOptions = {},
  ): Promise<CohortVersionResponse> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/cohort-versions/${encodeURIComponent(cohortVersionId)}`,
      signal: options.signal,
      validate: validateCohortVersion,
    });
  }

  async update(
    cohortVersionId: string,
    request: CohortVersionUpdateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<CohortVersionResponse> {
    return this.#transport.request({
      method: "PATCH",
      path: `/v1/cohort-versions/${encodeURIComponent(cohortVersionId)}`,
      body: jsonBody(request),
      signal: options.signal,
      validate: validateCohortVersion,
    });
  }

  async delete(
    cohortVersionId: string,
    options: ResourceRequestOptions = {},
  ): Promise<void> {
    return this.#transport.request({
      method: "DELETE",
      path: `/v1/cohort-versions/${encodeURIComponent(cohortVersionId)}`,
      responseType: "empty",
      signal: options.signal,
    });
  }
}
