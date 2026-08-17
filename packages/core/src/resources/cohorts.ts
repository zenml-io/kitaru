import { jsonBody, type KitaruTransport } from "../transport.js";
import type {
  CohortCreateRequest,
  CohortResponse,
  CohortUpdateRequest,
  CohortVersionCreateRequest,
  CohortVersionResponse,
  ListParams,
  Page,
} from "../types.js";
import {
  createPageValidator,
  validateCohort,
  validateCohortVersion,
} from "./internal.js";
import {
  encodeListParams,
  iteratePages,
  type ResourceRequestOptions,
} from "./pagination.js";

const validateCohortPage = createPageValidator(validateCohort);
const validateCohortVersionPage = createPageValidator(validateCohortVersion);

export class CohortsResource {
  readonly #transport: KitaruTransport;

  constructor(transport: KitaruTransport) {
    this.#transport = transport;
  }

  async create(
    request: CohortCreateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<CohortResponse> {
    return this.#transport.request({
      method: "POST",
      path: "/v1/cohorts",
      body: jsonBody(request),
      signal: options.signal,
      validate: validateCohort,
    });
  }

  async get(
    cohortId: string,
    options: ResourceRequestOptions = {},
  ): Promise<CohortResponse> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/cohorts/${encodeURIComponent(cohortId)}`,
      signal: options.signal,
      validate: validateCohort,
    });
  }

  async update(
    cohortId: string,
    request: CohortUpdateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<CohortResponse> {
    return this.#transport.request({
      method: "PATCH",
      path: `/v1/cohorts/${encodeURIComponent(cohortId)}`,
      body: jsonBody(request),
      signal: options.signal,
      validate: validateCohort,
    });
  }

  async delete(
    cohortId: string,
    options: ResourceRequestOptions = {},
  ): Promise<void> {
    return this.#transport.request({
      method: "DELETE",
      path: `/v1/cohorts/${encodeURIComponent(cohortId)}`,
      responseType: "empty",
      signal: options.signal,
    });
  }

  async list(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): Promise<Page<CohortResponse>> {
    return this.#transport.request({
      method: "GET",
      path: "/v1/cohorts",
      query: encodeListParams(params),
      signal: options.signal,
      validate: validateCohortPage,
    });
  }

  iter(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): AsyncIterable<CohortResponse> {
    return iteratePages(params, (pageParams) => this.list(pageParams, options));
  }

  async createVersion(
    cohortId: string,
    request: CohortVersionCreateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<CohortVersionResponse> {
    return this.#transport.request({
      method: "POST",
      path: `/v1/cohorts/${encodeURIComponent(cohortId)}/versions`,
      body: jsonBody(request),
      signal: options.signal,
      validate: validateCohortVersion,
    });
  }

  async listVersions(
    cohortId: string,
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): Promise<Page<CohortVersionResponse>> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/cohorts/${encodeURIComponent(cohortId)}/versions`,
      query: encodeListParams(params),
      signal: options.signal,
      validate: validateCohortVersionPage,
    });
  }

  iterVersions(
    cohortId: string,
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): AsyncIterable<CohortVersionResponse> {
    return iteratePages(params, (pageParams) =>
      this.listVersions(cohortId, pageParams, options),
    );
  }
}
