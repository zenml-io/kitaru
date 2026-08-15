import type { KitaruTransport } from "../transport.js";
import type {
  ExperimentRunResponse,
  JobResponse,
  ListParams,
  Page,
} from "../types.js";
import {
  createPageValidator,
  validateExperimentRun,
  validateJob,
} from "./internal.js";
import {
  encodeListParams,
  iteratePages,
  type ResourceRequestOptions,
} from "./pagination.js";
import { type WaitOptions, waitForTerminal } from "./wait.js";

const validateRunPage = createPageValidator(validateExperimentRun);
const validateJobPage = createPageValidator(validateJob);

export class ExperimentRunsResource {
  readonly #transport: KitaruTransport;

  constructor(transport: KitaruTransport) {
    this.#transport = transport;
  }

  async get(
    experimentRunId: string,
    options: ResourceRequestOptions = {},
  ): Promise<ExperimentRunResponse> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/experiment-runs/${encodeURIComponent(experimentRunId)}`,
      signal: options.signal,
      validate: validateExperimentRun,
    });
  }

  async list(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): Promise<Page<ExperimentRunResponse>> {
    return this.#transport.request({
      method: "GET",
      path: "/v1/experiment-runs",
      query: encodeListParams(params),
      signal: options.signal,
      validate: validateRunPage,
    });
  }

  iter(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): AsyncIterable<ExperimentRunResponse> {
    return iteratePages(params, (pageParams) => this.list(pageParams, options));
  }

  async listJobs(
    experimentRunId: string,
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): Promise<Page<JobResponse>> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/experiment-runs/${encodeURIComponent(experimentRunId)}/jobs`,
      query: encodeListParams(params),
      signal: options.signal,
      validate: validateJobPage,
    });
  }

  iterJobs(
    experimentRunId: string,
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): AsyncIterable<JobResponse> {
    return iteratePages(params, (pageParams) =>
      this.listJobs(experimentRunId, pageParams, options),
    );
  }

  async cancel(
    experimentRunId: string,
    options: ResourceRequestOptions = {},
  ): Promise<ExperimentRunResponse> {
    return this.#transport.request({
      method: "POST",
      path: `/v1/experiment-runs/${encodeURIComponent(experimentRunId)}/cancel`,
      signal: options.signal,
      validate: validateExperimentRun,
    });
  }

  async delete(
    experimentRunId: string,
    options: ResourceRequestOptions = {},
  ): Promise<void> {
    return this.#transport.request({
      method: "DELETE",
      path: `/v1/experiment-runs/${encodeURIComponent(experimentRunId)}`,
      responseType: "empty",
      signal: options.signal,
    });
  }

  wait(
    experimentRunId: string,
    options: WaitOptions = {},
  ): Promise<ExperimentRunResponse> {
    return waitForTerminal({
      get: (requestOptions) => this.get(experimentRunId, requestOptions),
      options,
      resource: "experiment-runs",
      resourceId: experimentRunId,
    });
  }
}
