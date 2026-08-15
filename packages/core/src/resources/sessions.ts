import type { QueryParameters } from "../query.js";
import { jsonBody, type KitaruTransport } from "../transport.js";
import type {
  ListParams,
  Page,
  SessionCreateRequest,
  SessionNodeBatchRequest,
  SessionNodeListParams,
  SessionNodeResponse,
  SessionResponse,
  SessionUpdateRequest,
  SessionWithNodesResponse,
} from "../types.js";
import { validateNode, validateNodes, validateSession } from "../validators.js";
import { createPageValidator, validateSessionWithNodes } from "./internal.js";
import {
  encodeListParams,
  iteratePages,
  type ResourceRequestOptions,
} from "./pagination.js";

const RETRYABLE_UPSERT_STATUSES = new Set([502, 503, 504]);
const validateSessionPage = createPageValidator(validateSession);
const validateNodePage = createPageValidator<SessionNodeResponse>(validateNode);

function encodeNodeListParams(params: SessionNodeListParams): QueryParameters {
  return {
    cursor: params.cursor,
    include_payloads: params.includePayloads,
    size: params.size,
  };
}

export class SessionsResource {
  readonly #transport: KitaruTransport;

  constructor(transport: KitaruTransport) {
    this.#transport = transport;
  }

  async create(
    request: SessionCreateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<SessionResponse> {
    return this.#transport.request({
      method: "POST",
      path: "/v1/sessions",
      body: jsonBody(request),
      signal: options.signal,
      validate: validateSession,
    });
  }

  async get(
    sessionId: string,
    options: ResourceRequestOptions = {},
  ): Promise<SessionResponse> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/sessions/${encodeURIComponent(sessionId)}`,
      signal: options.signal,
      validate: validateSession,
    });
  }

  async delete(
    sessionId: string,
    options: ResourceRequestOptions = {},
  ): Promise<void> {
    return this.#transport.request({
      method: "DELETE",
      path: `/v1/sessions/${encodeURIComponent(sessionId)}`,
      responseType: "empty",
      signal: options.signal,
    });
  }

  async update(
    sessionId: string,
    request: SessionUpdateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<SessionResponse> {
    return this.#transport.request({
      method: "PATCH",
      path: `/v1/sessions/${encodeURIComponent(sessionId)}`,
      body: jsonBody(request),
      signal: options.signal,
      validate: validateSession,
    });
  }

  async list(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): Promise<Page<SessionResponse>> {
    return this.#transport.request({
      method: "GET",
      path: "/v1/sessions",
      query: encodeListParams(params),
      signal: options.signal,
      validate: validateSessionPage,
    });
  }

  iter(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): AsyncIterable<SessionResponse> {
    return iteratePages(params, (pageParams) => this.list(pageParams, options));
  }

  async getWithNodes(
    sessionId: string,
    options: ResourceRequestOptions = {},
  ): Promise<SessionWithNodesResponse> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/sessions/${encodeURIComponent(sessionId)}/full`,
      signal: options.signal,
      validate: validateSessionWithNodes,
    });
  }

  async listNodes(
    sessionId: string,
    params: SessionNodeListParams = {},
    options: ResourceRequestOptions = {},
  ): Promise<Page<SessionNodeResponse>> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/sessions/${encodeURIComponent(sessionId)}/nodes`,
      query: encodeNodeListParams(params),
      signal: options.signal,
      validate: validateNodePage,
    });
  }

  iterNodes(
    sessionId: string,
    params: SessionNodeListParams = {},
    options: ResourceRequestOptions = {},
  ): AsyncIterable<SessionNodeResponse> {
    return iteratePages(params, (pageParams) =>
      this.listNodes(sessionId, pageParams, options),
    );
  }

  async upsertNodes(
    sessionId: string,
    request: SessionNodeBatchRequest,
    options: ResourceRequestOptions = {},
  ): Promise<SessionNodeResponse[]> {
    return this.#transport.request({
      method: "POST",
      path: `/v1/sessions/${encodeURIComponent(sessionId)}/nodes`,
      body: jsonBody(request),
      retry: {
        attempts: 2,
        retryTransportErrors: true,
        statuses: RETRYABLE_UPSERT_STATUSES,
      },
      signal: options.signal,
      validate: validateNodes,
    });
  }
}
