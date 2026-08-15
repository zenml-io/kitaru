import { KitaruApiError } from "./errors.js";
import type { HttpMethod } from "./transport.js";

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function isUuid(value: string): boolean {
  return UUID_PATTERN.test(value);
}

export function invalidResponse(
  method: HttpMethod,
  path: string,
  status: number,
  detail: string,
): never {
  throw new KitaruApiError(
    method,
    path,
    status,
    `Invalid response: ${detail}`,
    { kind: "validation" },
  );
}

export function requireResponseString(
  value: Record<string, unknown>,
  property: string,
  method: HttpMethod,
  path: string,
  status: number,
  missingDetail = `invalid ${property}`,
): void {
  if (typeof value[property] !== "string" || value[property].length === 0) {
    invalidResponse(method, path, status, missingDetail);
  }
}

export function requireResponseUuid(
  value: Record<string, unknown>,
  property: string,
  method: HttpMethod,
  path: string,
  status: number,
  missingDetail = `invalid ${property}`,
): void {
  requireResponseString(value, property, method, path, status, missingDetail);
  if (!isUuid(value[property] as string)) {
    invalidResponse(method, path, status, `invalid ${property}`);
  }
}

export function requireResponseEnum(
  value: Record<string, unknown>,
  property: string,
  allowed: ReadonlySet<string>,
  method: HttpMethod,
  path: string,
  status: number,
  missingDetail = `invalid ${property}`,
): void {
  requireResponseString(value, property, method, path, status, missingDetail);
  if (!allowed.has(value[property] as string)) {
    invalidResponse(method, path, status, `invalid ${property}`);
  }
}
