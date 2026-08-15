import type { KitaruTransport } from "../transport.js";
import type {
  ListParams,
  Page,
  TaskResponse,
  TaskSpecResponse,
} from "../types.js";
import { validateTaskSpec } from "../validators.js";
import { createPageValidator, validateTask } from "./internal.js";
import {
  encodeListParams,
  iteratePages,
  type ResourceRequestOptions,
} from "./pagination.js";

const validateTaskPage = createPageValidator(validateTask);

export class TasksResource {
  readonly #transport: KitaruTransport;

  constructor(transport: KitaruTransport) {
    this.#transport = transport;
  }

  async get(
    taskId: string,
    options: ResourceRequestOptions = {},
  ): Promise<TaskResponse> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/tasks/${encodeURIComponent(taskId)}`,
      signal: options.signal,
      validate: validateTask,
    });
  }

  async getSpec(
    taskId: string,
    options: ResourceRequestOptions = {},
  ): Promise<TaskSpecResponse> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/tasks/${encodeURIComponent(taskId)}/spec`,
      signal: options.signal,
      validate: validateTaskSpec,
    });
  }

  async list(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): Promise<Page<TaskResponse>> {
    return this.#transport.request({
      method: "GET",
      path: "/v1/tasks",
      query: encodeListParams(params),
      signal: options.signal,
      validate: validateTaskPage,
    });
  }

  iter(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): AsyncIterable<TaskResponse> {
    return iteratePages(params, (pageParams) => this.list(pageParams, options));
  }
}
