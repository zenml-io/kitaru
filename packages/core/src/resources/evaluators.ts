import { jsonBody, type KitaruTransport } from "../transport.js";
import type {
  EvaluatorCreateRequest,
  EvaluatorResponse,
  EvaluatorUpdateRequest,
  EvaluatorVersionCreateRequest,
  EvaluatorVersionResponse,
  EvaluatorVersionUpdateRequest,
  ListParams,
  Page,
  UnfilteredListParams,
} from "../types.js";
import {
  createPageValidator,
  validateEvaluator,
  validateEvaluatorVersion,
} from "./internal.js";
import {
  encodeListParams,
  iteratePages,
  type ResourceRequestOptions,
} from "./pagination.js";

const validateEvaluatorPage = createPageValidator(validateEvaluator);
const validateEvaluatorVersionPage = createPageValidator(
  validateEvaluatorVersion,
);

export class EvaluatorsResource {
  readonly #transport: KitaruTransport;

  constructor(transport: KitaruTransport) {
    this.#transport = transport;
  }

  async create(
    request: EvaluatorCreateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<EvaluatorResponse> {
    return this.#transport.request({
      method: "POST",
      path: "/v1/evaluators",
      body: jsonBody(request),
      signal: options.signal,
      validate: validateEvaluator,
    });
  }

  async get(
    evaluatorId: string,
    options: ResourceRequestOptions = {},
  ): Promise<EvaluatorResponse> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/evaluators/${encodeURIComponent(evaluatorId)}`,
      signal: options.signal,
      validate: validateEvaluator,
    });
  }

  async update(
    evaluatorId: string,
    request: EvaluatorUpdateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<EvaluatorResponse> {
    return this.#transport.request({
      method: "PATCH",
      path: `/v1/evaluators/${encodeURIComponent(evaluatorId)}`,
      body: jsonBody(request),
      signal: options.signal,
      validate: validateEvaluator,
    });
  }

  async delete(
    evaluatorId: string,
    options: ResourceRequestOptions = {},
  ): Promise<void> {
    return this.#transport.request({
      method: "DELETE",
      path: `/v1/evaluators/${encodeURIComponent(evaluatorId)}`,
      responseType: "empty",
      signal: options.signal,
    });
  }

  async list(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): Promise<Page<EvaluatorResponse>> {
    return this.#transport.request({
      method: "GET",
      path: "/v1/evaluators",
      query: encodeListParams(params),
      signal: options.signal,
      validate: validateEvaluatorPage,
    });
  }

  iter(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): AsyncIterable<EvaluatorResponse> {
    return iteratePages(params, (pageParams) => this.list(pageParams, options));
  }

  async createVersion(
    evaluatorId: string,
    request: EvaluatorVersionCreateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<EvaluatorVersionResponse> {
    return this.#transport.request({
      method: "POST",
      path: `/v1/evaluators/${encodeURIComponent(evaluatorId)}/versions`,
      body: jsonBody(request),
      signal: options.signal,
      validate: validateEvaluatorVersion,
    });
  }

  async getVersion(
    evaluatorId: string,
    version: number,
    options: ResourceRequestOptions = {},
  ): Promise<EvaluatorVersionResponse> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/evaluators/${encodeURIComponent(evaluatorId)}/versions/${encodeURIComponent(String(version))}`,
      signal: options.signal,
      validate: validateEvaluatorVersion,
    });
  }

  async updateVersion(
    evaluatorId: string,
    version: number,
    request: EvaluatorVersionUpdateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<EvaluatorVersionResponse> {
    return this.#transport.request({
      method: "PATCH",
      path: `/v1/evaluators/${encodeURIComponent(evaluatorId)}/versions/${encodeURIComponent(String(version))}`,
      body: jsonBody(request),
      signal: options.signal,
      validate: validateEvaluatorVersion,
    });
  }

  async listVersions(
    evaluatorId: string,
    params: UnfilteredListParams = {},
    options: ResourceRequestOptions = {},
  ): Promise<Page<EvaluatorVersionResponse>> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/evaluators/${encodeURIComponent(evaluatorId)}/versions`,
      query: encodeListParams(params),
      signal: options.signal,
      validate: validateEvaluatorVersionPage,
    });
  }

  iterVersions(
    evaluatorId: string,
    params: UnfilteredListParams = {},
    options: ResourceRequestOptions = {},
  ): AsyncIterable<EvaluatorVersionResponse> {
    return iteratePages(params, (pageParams) =>
      this.listVersions(evaluatorId, pageParams, options),
    );
  }
}
