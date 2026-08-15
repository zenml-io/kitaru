import { KitaruClient, type KitaruClientOptions } from "../client.js";
import { resolveKitaruEnvironment } from "../environment.js";
import {
  credentialsCacheDisabled,
  readSelectedServerUrl,
  resolveConfigDirectory,
  StoredLoginCredentialProvider,
  type StoredLoginEnvironment,
} from "./stored-login.js";

export interface NodeKitaruClientOptions extends KitaruClientOptions {
  environment?: StoredLoginEnvironment;
  homeDirectory?: string;
}

/** Create a client that may reuse the Python CLI's selected login read-only. */
export async function createKitaruClient(
  options: NodeKitaruClientOptions = {},
): Promise<KitaruClient> {
  const environment = options.environment ?? process.env;
  const configDirectory = resolveConfigDirectory(
    environment,
    options.homeDirectory,
  );
  const apiUrl =
    options.apiUrl ??
    environment.KITARU_API_URL ??
    (await readSelectedServerUrl(configDirectory));
  const explicitToken =
    options.apiKey ??
    (options.credentialProvider === undefined
      ? (environment.KITARU_API_TOKEN ?? environment.KITARU_API_KEY)
      : undefined);
  const timeoutMs = resolveKitaruEnvironment(
    { apiUrl, timeoutMs: options.timeoutMs },
    {},
  ).timeoutMs;

  let credentialProvider = options.credentialProvider;
  if (explicitToken === undefined && credentialProvider === undefined) {
    if (credentialsCacheDisabled(environment)) {
      throw new Error(
        "Stored Kitaru credential cache is disabled and no explicit credential was provided",
      );
    }
    credentialProvider = new StoredLoginCredentialProvider({
      apiUrl,
      configDirectory,
      fetch: options.fetch,
      timeoutMs,
    });
  }

  return new KitaruClient({
    apiKey: explicitToken,
    apiUrl,
    credentialProvider,
    environment,
    fetch: options.fetch,
    timeoutMs,
  });
}

export type {
  StoredLoginEnvironment,
  StoredLoginOptions,
} from "./stored-login.js";
export {
  credentialsCacheDisabled,
  readSelectedServerUrl,
  resolveConfigDirectory,
  StoredLoginCredentialProvider,
} from "./stored-login.js";
