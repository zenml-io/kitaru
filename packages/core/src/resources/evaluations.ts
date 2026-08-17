import { jsonBody, type KitaruTransport } from "../transport.js";
import type {
  EvaluationBatchCreateRequest,
  EvaluationResponse,
  JobResponse,
  ListParams,
  Page,
} from "../types.js";
import {
  createPageValidator,
  validateEvaluation,
  validateJob,
} from "./internal.js";
import {
  encodeListParams,
  iteratePages,
  type ResourceRequestOptions,
} from "./pagination.js";

const validateEvaluationPage = createPageValidator(validateEvaluation);

export class EvaluationsResource {
  readonly #transport: KitaruTransport;

  constructor(transport: KitaruTransport) {
    this.#transport = transport;
  }

  async create(
    request: EvaluationBatchCreateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<JobResponse> {
    return this.#transport.request({
      method: "POST",
      path: "/v1/evaluations",
      body: jsonBody(request),
      signal: options.signal,
      validate: validateJob,
    });
  }

  async get(
    evaluationId: string,
    options: ResourceRequestOptions = {},
  ): Promise<EvaluationResponse> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/evaluations/${encodeURIComponent(evaluationId)}`,
      signal: options.signal,
      validate: validateEvaluation,
    });
  }

  async list(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): Promise<Page<EvaluationResponse>> {
    return this.#transport.request({
      method: "GET",
      path: "/v1/evaluations",
      query: encodeListParams(params),
      signal: options.signal,
      validate: validateEvaluationPage,
    });
  }

  iter(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): AsyncIterable<EvaluationResponse> {
    return iteratePages(params, (pageParams) => this.list(pageParams, options));
  }
}
