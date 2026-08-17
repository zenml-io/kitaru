export class ToolPolicyError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "ToolPolicyError";
  }
}

export class ToolPolicyMissError extends ToolPolicyError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "ToolPolicyMissError";
  }
}

export type KitaruApiErrorKind =
  | "api"
  | "canceled"
  | "redirect"
  | "timeout"
  | "transport"
  | "validation";

export interface KitaruApiErrorOptions extends ErrorOptions {
  kind?: KitaruApiErrorKind;
}

export class KitaruApiError extends Error {
  readonly kind: KitaruApiErrorKind;
  readonly method: string;
  readonly path: string;
  readonly status: number | null;

  constructor(
    method: string,
    path: string,
    status: number | null,
    detail: string,
    options?: KitaruApiErrorOptions,
  ) {
    super(`${method} ${path}: ${detail}`, options);
    this.name = "KitaruApiError";
    this.kind = options?.kind ?? "api";
    this.method = method;
    this.path = path;
    this.status = status;
  }

  toJSON(): Record<string, unknown> {
    return {
      kind: this.kind,
      message: this.message,
      method: this.method,
      name: this.name,
      path: this.path,
      status: this.status,
    };
  }
}

export type KitaruWaitErrorKind = "canceled" | "timeout";

export interface KitaruWaitErrorOptions<T> extends ErrorOptions {
  kind: KitaruWaitErrorKind;
  lastState?: T;
}

export class KitaruWaitError<T> extends KitaruApiError {
  readonly lastState: T | undefined;
  readonly remoteContinues = true;
  readonly resource: string;
  readonly resourceId: string;

  constructor(
    resource: string,
    resourceId: string,
    options: KitaruWaitErrorOptions<T>,
  ) {
    const action = options.kind === "canceled" ? "Canceled" : "Timed out";
    super(
      "GET",
      `/api/v1/${resource}/${encodeURIComponent(resourceId)}`,
      null,
      `${action} waiting for ${resource} ${resourceId}; remote work continues`,
      { cause: options.cause, kind: options.kind },
    );
    this.name = "KitaruWaitError";
    this.lastState = options.lastState;
    this.resource = resource;
    this.resourceId = resourceId;
  }

  override toJSON(): Record<string, unknown> {
    const lastStatus =
      typeof this.lastState === "object" &&
      this.lastState !== null &&
      "status" in this.lastState &&
      typeof this.lastState.status === "string"
        ? this.lastState.status
        : undefined;
    return {
      ...super.toJSON(),
      lastStatus,
      remoteContinues: this.remoteContinues,
      resource: this.resource,
      resourceId: this.resourceId,
    };
  }
}
