import { jsonBody, type KitaruTransport } from "../transport.js";
import type {
  AgentCreateRequest,
  AgentResponse,
  AgentUpdateRequest,
  AgentVersionCreateRequest,
  AgentVersionResponse,
  AgentVersionUpdateRequest,
  ListParams,
  Page,
} from "../types.js";
import {
  createPageValidator,
  validateAgent,
  validateAgentVersion,
} from "./internal.js";
import {
  encodeListParams,
  iteratePages,
  type ResourceRequestOptions,
} from "./pagination.js";

const validateAgentPage = createPageValidator(validateAgent);
const validateAgentVersionPage = createPageValidator(validateAgentVersion);

export class AgentsResource {
  readonly #transport: KitaruTransport;

  constructor(transport: KitaruTransport) {
    this.#transport = transport;
  }

  async create(
    request: AgentCreateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<AgentResponse> {
    return this.#transport.request({
      method: "POST",
      path: "/v1/agents",
      body: jsonBody(request),
      signal: options.signal,
      validate: validateAgent,
    });
  }

  async get(
    agentId: string,
    options: ResourceRequestOptions = {},
  ): Promise<AgentResponse> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/agents/${encodeURIComponent(agentId)}`,
      signal: options.signal,
      validate: validateAgent,
    });
  }

  async update(
    agentId: string,
    request: AgentUpdateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<AgentResponse> {
    return this.#transport.request({
      method: "PATCH",
      path: `/v1/agents/${encodeURIComponent(agentId)}`,
      body: jsonBody(request),
      signal: options.signal,
      validate: validateAgent,
    });
  }

  async delete(
    agentId: string,
    options: ResourceRequestOptions = {},
  ): Promise<void> {
    return this.#transport.request({
      method: "DELETE",
      path: `/v1/agents/${encodeURIComponent(agentId)}`,
      responseType: "empty",
      signal: options.signal,
    });
  }

  async list(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): Promise<Page<AgentResponse>> {
    return this.#transport.request({
      method: "GET",
      path: "/v1/agents",
      query: encodeListParams(params),
      signal: options.signal,
      validate: validateAgentPage,
    });
  }

  iter(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): AsyncIterable<AgentResponse> {
    return iteratePages(params, (pageParams) => this.list(pageParams, options));
  }

  async createVersion(
    agentId: string,
    request: AgentVersionCreateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<AgentVersionResponse> {
    return this.#transport.request({
      method: "POST",
      path: `/v1/agents/${encodeURIComponent(agentId)}/versions`,
      body: jsonBody(request),
      signal: options.signal,
      validate: validateAgentVersion,
    });
  }

  async getVersion(
    agentVersionId: string,
    options: ResourceRequestOptions = {},
  ): Promise<AgentVersionResponse> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/agent-versions/${encodeURIComponent(agentVersionId)}`,
      signal: options.signal,
      validate: validateAgentVersion,
    });
  }

  async updateVersion(
    agentVersionId: string,
    request: AgentVersionUpdateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<AgentVersionResponse> {
    return this.#transport.request({
      method: "PATCH",
      path: `/v1/agent-versions/${encodeURIComponent(agentVersionId)}`,
      body: jsonBody(request),
      signal: options.signal,
      validate: validateAgentVersion,
    });
  }

  async deleteVersion(
    agentVersionId: string,
    options: ResourceRequestOptions = {},
  ): Promise<void> {
    return this.#transport.request({
      method: "DELETE",
      path: `/v1/agent-versions/${encodeURIComponent(agentVersionId)}`,
      responseType: "empty",
      signal: options.signal,
    });
  }

  async listVersions(
    agentId: string,
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): Promise<Page<AgentVersionResponse>> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/agents/${encodeURIComponent(agentId)}/versions`,
      query: encodeListParams(params),
      signal: options.signal,
      validate: validateAgentVersionPage,
    });
  }

  iterVersions(
    agentId: string,
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): AsyncIterable<AgentVersionResponse> {
    return iteratePages(params, (pageParams) =>
      this.listVersions(agentId, pageParams, options),
    );
  }
}
