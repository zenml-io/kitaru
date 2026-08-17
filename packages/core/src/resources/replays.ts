import { jsonBody, type KitaruTransport } from "../transport.js";
import type {
  ListParams,
  Page,
  ReplayCreateRequest,
  ReplayResponse,
  ToolLookupRequest,
  ToolLookupResponse,
} from "../types.js";
import { validateReplay, validateToolLookup } from "../validators.js";
import { createPageValidator } from "./internal.js";
import {
  encodeListParams,
  iteratePages,
  type ResourceRequestOptions,
} from "./pagination.js";
import { type WaitOptions, waitForTerminal } from "./wait.js";

const validateReplayPage = createPageValidator(validateReplay);

export class ReplaysResource {
  readonly #transport: KitaruTransport;

  constructor(transport: KitaruTransport) {
    this.#transport = transport;
  }

  async create(
    request: ReplayCreateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<ReplayResponse> {
    return this.#transport.request({
      method: "POST",
      path: "/api/v1/replays",
      body: jsonBody(request),
      signal: options.signal,
      validate: validateReplay,
    });
  }

  async get(
    replayId: string,
    options: ResourceRequestOptions = {},
  ): Promise<ReplayResponse> {
    return this.#transport.request({
      method: "GET",
      path: `/api/v1/replays/${encodeURIComponent(replayId)}`,
      signal: options.signal,
      validate: validateReplay,
    });
  }

  async list(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): Promise<Page<ReplayResponse>> {
    return this.#transport.request({
      method: "GET",
      path: "/api/v1/replays",
      query: encodeListParams(params),
      signal: options.signal,
      validate: validateReplayPage,
    });
  }

  iter(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): AsyncIterable<ReplayResponse> {
    return iteratePages(params, (pageParams) => this.list(pageParams, options));
  }

  async toolLookup(
    replayId: string,
    request: ToolLookupRequest,
    options: ResourceRequestOptions = {},
  ): Promise<ToolLookupResponse> {
    return this.#transport.request({
      method: "POST",
      path: `/api/v1/replays/${encodeURIComponent(replayId)}/tool-lookup`,
      body: jsonBody(request),
      signal: options.signal,
      validate: validateToolLookup,
    });
  }

  wait(replayId: string, options: WaitOptions = {}): Promise<ReplayResponse> {
    return waitForTerminal({
      get: (requestOptions) => this.get(replayId, requestOptions),
      options,
      resource: "replays",
      resourceId: replayId,
    });
  }
}
