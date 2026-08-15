import type { KitaruTransport } from "../transport.js";
import type { JobResponse, ListParams, Page, TaskResponse } from "../types.js";
import { createPageValidator, validateJob, validateTask } from "./internal.js";
import {
  encodeListParams,
  iteratePages,
  type ResourceRequestOptions,
} from "./pagination.js";
import { type WaitOptions, waitForTerminal } from "./wait.js";

const validateJobPage = createPageValidator(validateJob);
const validateTaskPage = createPageValidator(validateTask);

export class JobsResource {
  readonly #transport: KitaruTransport;

  constructor(transport: KitaruTransport) {
    this.#transport = transport;
  }

  async get(
    jobId: string,
    options: ResourceRequestOptions = {},
  ): Promise<JobResponse> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/jobs/${encodeURIComponent(jobId)}`,
      signal: options.signal,
      validate: validateJob,
    });
  }

  async list(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): Promise<Page<JobResponse>> {
    return this.#transport.request({
      method: "GET",
      path: "/v1/jobs",
      query: encodeListParams(params),
      signal: options.signal,
      validate: validateJobPage,
    });
  }

  iter(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): AsyncIterable<JobResponse> {
    return iteratePages(params, (pageParams) => this.list(pageParams, options));
  }

  async listTasks(
    jobId: string,
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): Promise<Page<TaskResponse>> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/jobs/${encodeURIComponent(jobId)}/tasks`,
      query: encodeListParams(params),
      signal: options.signal,
      validate: validateTaskPage,
    });
  }

  iterTasks(
    jobId: string,
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): AsyncIterable<TaskResponse> {
    return iteratePages(params, (pageParams) =>
      this.listTasks(jobId, pageParams, options),
    );
  }

  async cancel(
    jobId: string,
    options: ResourceRequestOptions = {},
  ): Promise<JobResponse> {
    return this.#transport.request({
      method: "POST",
      path: `/v1/jobs/${encodeURIComponent(jobId)}/cancel`,
      signal: options.signal,
      validate: validateJob,
    });
  }

  async delete(
    jobId: string,
    options: ResourceRequestOptions = {},
  ): Promise<void> {
    return this.#transport.request({
      method: "DELETE",
      path: `/v1/jobs/${encodeURIComponent(jobId)}`,
      responseType: "empty",
      signal: options.signal,
    });
  }

  wait(jobId: string, options: WaitOptions = {}): Promise<JobResponse> {
    return waitForTerminal({
      get: (requestOptions) => this.get(jobId, requestOptions),
      options,
      resource: "jobs",
      resourceId: jobId,
    });
  }
}
