import { jsonBody, type KitaruTransport } from "../transport.js";
import type {
  ExperimentCreateRequest,
  ExperimentResponse,
  ExperimentRunCreateRequest,
  ExperimentRunResponse,
  ExperimentUpdateRequest,
  ListParams,
  Page,
} from "../types.js";
import {
  createPageValidator,
  validateExperiment,
  validateExperimentRun,
} from "./internal.js";
import {
  encodeListParams,
  iteratePages,
  type ResourceRequestOptions,
} from "./pagination.js";

const validateExperimentPage = createPageValidator(validateExperiment);

export class ExperimentsResource {
  readonly #transport: KitaruTransport;

  constructor(transport: KitaruTransport) {
    this.#transport = transport;
  }

  async create(
    request: ExperimentCreateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<ExperimentResponse> {
    return this.#transport.request({
      method: "POST",
      path: "/api/v1/experiments",
      body: jsonBody(request),
      signal: options.signal,
      validate: validateExperiment,
    });
  }

  async get(
    experimentId: string,
    options: ResourceRequestOptions = {},
  ): Promise<ExperimentResponse> {
    return this.#transport.request({
      method: "GET",
      path: `/api/v1/experiments/${encodeURIComponent(experimentId)}`,
      signal: options.signal,
      validate: validateExperiment,
    });
  }

  async list(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): Promise<Page<ExperimentResponse>> {
    return this.#transport.request({
      method: "GET",
      path: "/api/v1/experiments",
      query: encodeListParams(params),
      signal: options.signal,
      validate: validateExperimentPage,
    });
  }

  iter(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): AsyncIterable<ExperimentResponse> {
    return iteratePages(params, (pageParams) => this.list(pageParams, options));
  }

  async update(
    experimentId: string,
    request: ExperimentUpdateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<ExperimentResponse> {
    return this.#transport.request({
      method: "PATCH",
      path: `/api/v1/experiments/${encodeURIComponent(experimentId)}`,
      body: jsonBody(request),
      signal: options.signal,
      validate: validateExperiment,
    });
  }

  async delete(
    experimentId: string,
    options: ResourceRequestOptions = {},
  ): Promise<void> {
    return this.#transport.request({
      method: "DELETE",
      path: `/api/v1/experiments/${encodeURIComponent(experimentId)}`,
      responseType: "empty",
      signal: options.signal,
    });
  }

  async startRun(
    experimentId: string,
    request: ExperimentRunCreateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<ExperimentRunResponse> {
    return this.#transport.request({
      method: "POST",
      path: `/api/v1/experiments/${encodeURIComponent(experimentId)}/runs`,
      body: jsonBody(request),
      signal: options.signal,
      validate: validateExperimentRun,
    });
  }
}
