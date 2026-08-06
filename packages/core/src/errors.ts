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

export class KitaruApiError extends Error {
  readonly method: string;
  readonly path: string;
  readonly status: number | null;

  constructor(
    method: string,
    path: string,
    status: number | null,
    detail: string,
    options?: ErrorOptions,
  ) {
    super(`${method} ${path}: ${detail}`, options);
    this.name = "KitaruApiError";
    this.method = method;
    this.path = path;
    this.status = status;
  }
}
