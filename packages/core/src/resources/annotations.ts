import { jsonBody, type KitaruTransport } from "../transport.js";
import type {
  AnnotationCreateRequest,
  AnnotationResponse,
  AnnotationUpdateRequest,
  ListParams,
  Page,
} from "../types.js";
import { createPageValidator, validateAnnotation } from "./internal.js";
import {
  encodeListParams,
  iteratePages,
  type ResourceRequestOptions,
} from "./pagination.js";

const validateAnnotationPage = createPageValidator(validateAnnotation);

export class AnnotationsResource {
  readonly #transport: KitaruTransport;

  constructor(transport: KitaruTransport) {
    this.#transport = transport;
  }

  async create(
    request: AnnotationCreateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<AnnotationResponse> {
    return this.#transport.request({
      method: "POST",
      path: "/v1/annotations",
      body: jsonBody(request),
      signal: options.signal,
      validate: validateAnnotation,
    });
  }

  async get(
    annotationId: string,
    options: ResourceRequestOptions = {},
  ): Promise<AnnotationResponse> {
    return this.#transport.request({
      method: "GET",
      path: `/v1/annotations/${encodeURIComponent(annotationId)}`,
      signal: options.signal,
      validate: validateAnnotation,
    });
  }

  async update(
    annotationId: string,
    request: AnnotationUpdateRequest,
    options: ResourceRequestOptions = {},
  ): Promise<AnnotationResponse> {
    return this.#transport.request({
      method: "PATCH",
      path: `/v1/annotations/${encodeURIComponent(annotationId)}`,
      body: jsonBody(request),
      signal: options.signal,
      validate: validateAnnotation,
    });
  }

  async delete(
    annotationId: string,
    options: ResourceRequestOptions = {},
  ): Promise<void> {
    return this.#transport.request({
      method: "DELETE",
      path: `/v1/annotations/${encodeURIComponent(annotationId)}`,
      responseType: "empty",
      signal: options.signal,
    });
  }

  async list(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): Promise<Page<AnnotationResponse>> {
    return this.#transport.request({
      method: "GET",
      path: "/v1/annotations",
      query: encodeListParams(params),
      signal: options.signal,
      validate: validateAnnotationPage,
    });
  }

  iter(
    params: ListParams = {},
    options: ResourceRequestOptions = {},
  ): AsyncIterable<AnnotationResponse> {
    return iteratePages(params, (pageParams) => this.list(pageParams, options));
  }
}
