import { jsonBody, type KitaruTransport } from "../transport.js";
import type {
  InvestigationCreateRequest,
  InvestigationResponse,
  InvestigationSessionListParams,
  InvestigationSessionResponse,
  InvestigationSessionUpdateRequest,
  InvestigationUpdateRequest,
  ListParams,
  Page,
} from "../types.js";
import {
  createPageValidator,
  validateInvestigation,
  validateInvestigationSession,
} from "./internal.js";
import {
  encodeListParams,
  iteratePages,
  type ResourceRequestOptions,
} from "./pagination.js";

const validateInvestigationPage = createPageValidator(validateInvestigation);
const validateInvestigationSessionPage = createPageValidator(
  validateInvestigationSession,
);

export class InvestigationsResource {
  readonly #transport: KitaruTransport;

  constructor(transport: KitaruTransport) {
    this.#transport = transport;
  }

  async create(
    request: InvestigationCreateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<InvestigationResponse> {
    return this.#transport.request({
      method: "POST",
      path: "/v1/investigations",
      body: jsonBody(request),
      signal: options.signal,
      validate: validateInvestigation,
    });
  }

  async get(
    investigationId: string,
    options: ResourceRequestOptions = {},
  ): Promise<InvestigationResponse> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/investigations/${encodeURIComponent(investigationId)}`,
      signal: options.signal,
      validate: validateInvestigation,
    });
  }

  async update(
    investigationId: string,
    request: InvestigationUpdateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<InvestigationResponse> {
    return this.#transport.request({
      method: "PATCH",
      path: `/v1/investigations/${encodeURIComponent(investigationId)}`,
      body: jsonBody(request),
      signal: options.signal,
      validate: validateInvestigation,
    });
  }

  async delete(
    investigationId: string,
    options: ResourceRequestOptions = {},
  ): Promise<void> {
    return this.#transport.request({
      method: "DELETE",
      path: `/v1/investigations/${encodeURIComponent(investigationId)}`,
      responseType: "empty",
      signal: options.signal,
    });
  }

  async list(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): Promise<Page<InvestigationResponse>> {
    return this.#transport.request({
      method: "GET",
      path: "/v1/investigations",
      query: encodeListParams(params),
      signal: options.signal,
      validate: validateInvestigationPage,
    });
  }

  iter(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): AsyncIterable<InvestigationResponse> {
    return iteratePages(params, (pageParams) => this.list(pageParams, options));
  }

  async listSessions(
    investigationId: string,
    params: InvestigationSessionListParams = {},
    options: ResourceRequestOptions = {},
  ): Promise<Page<InvestigationSessionResponse>> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/investigations/${encodeURIComponent(investigationId)}/sessions`,
      query: { cursor: params.cursor, size: params.size },
      signal: options.signal,
      validate: validateInvestigationSessionPage,
    });
  }

  iterSessions(
    investigationId: string,
    params: InvestigationSessionListParams = {},
    options: ResourceRequestOptions = {},
  ): AsyncIterable<InvestigationSessionResponse> {
    return iteratePages(params, (pageParams) =>
      this.listSessions(investigationId, pageParams, options),
    );
  }

  async updateSession(
    investigationId: string,
    sessionId: string,
    request: InvestigationSessionUpdateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<InvestigationSessionResponse> {
    return this.#transport.request({
      method: "PATCH",
      path: `/v1/investigations/${encodeURIComponent(investigationId)}/sessions/${encodeURIComponent(sessionId)}`,
      body: jsonBody(request),
      signal: options.signal,
      validate: validateInvestigationSession,
    });
  }
}
