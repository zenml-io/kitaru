export interface KitaruEnvironmentOptions {
  apiKey?: string;
  apiUrl?: string;
  timeoutMs?: number;
}

export type KitaruEnvironmentVariables = Readonly<
  Record<string, string | undefined>
>;

export interface KitaruEnvironment {
  apiKey?: string;
  apiUrl: string;
  timeoutMs: number;
}

const DEFAULT_TIMEOUT_MS = 30_000;

function getProcessEnvironment(): KitaruEnvironmentVariables {
  return typeof process === "undefined" ? {} : (process.env ?? {});
}

export function resolveKitaruEnvironment(
  options: KitaruEnvironmentOptions = {},
  environment: KitaruEnvironmentVariables = getProcessEnvironment(),
): KitaruEnvironment {
  const apiUrl = options.apiUrl ?? environment.KITARU_API_URL;
  if (!apiUrl) {
    throw new Error("KITARU_API_URL is not set");
  }

  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    throw new Error("timeoutMs must be a positive finite number");
  }

  return {
    apiUrl: apiUrl.replace(/\/$/, ""),
    apiKey:
      options.apiKey ??
      environment.KITARU_API_TOKEN ??
      environment.KITARU_API_KEY,
    timeoutMs,
  };
}
