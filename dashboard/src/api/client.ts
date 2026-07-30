import createClient from "openapi-fetch";
import { getApiKey } from "../lib/settings";
import type { paths } from "./schema";

// Read-only dashboard rule: never fetch /v1/tasks/{id}/spec or secret values
// (/v1/secrets with include_values) — both surface decrypted secret material.

// Relative base URL: every request goes through the Vite dev proxy so the
// browser stays on a single origin (the server has no CORS middleware).
export const client = createClient<paths>({ baseUrl: "" });

client.use({
  onRequest({ request }) {
    const apiKey = getApiKey();
    if (apiKey) {
      request.headers.set("Authorization", `Bearer ${apiKey}`);
    }
    return request;
  },
});

export class ApiError extends Error {
  readonly status: number;
  readonly detail: string;

  constructor(status: number, detail: string) {
    super(`${status}: ${detail}`);
    this.name = "ApiError";
    this.status = status;
    this.detail = detail;
  }
}

interface ValidationErrorItem {
  loc?: (string | number)[];
  msg?: string;
}

function formatDetail(error: unknown): string {
  if (typeof error === "string") {
    return error;
  }
  if (error && typeof error === "object" && "detail" in error) {
    const detail = (error as { detail: unknown }).detail;
    if (typeof detail === "string") {
      return detail;
    }
    if (Array.isArray(detail)) {
      return detail
        .map((item: ValidationErrorItem) => {
          const location = item.loc?.join(".") ?? "";
          return location
            ? `${location}: ${item.msg ?? "invalid"}`
            : (item.msg ?? "invalid");
        })
        .join("; ");
    }
  }
  return "Request failed";
}

/** Turn openapi-fetch's {data, error} result into throw-on-error for React Query. */
export async function unwrap<T>(
  promise: Promise<{ data?: T; error?: unknown; response: Response }>,
): Promise<T> {
  const { data, error, response } = await promise;
  if (error !== undefined || data === undefined) {
    throw new ApiError(response.status, formatDetail(error));
  }
  return data;
}
