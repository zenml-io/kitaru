import { awaitWithSignal } from "../abort.js";

/** A bearer credential bound to one immutable authentication identity. */
export interface ResolvedCredential {
  token: string;
  identity: string;
  generation: number;
}

/** Runtime-neutral asynchronous credential lookup. */
export interface AsyncCredentialProvider {
  getCredential(signal: AbortSignal): Promise<ResolvedCredential | undefined>;
}

/** A provider that can replace a rejected bearer for the same identity. */
export interface RenewableCredentialProvider extends AsyncCredentialProvider {
  renewCredential(
    rejected: ResolvedCredential,
    signal: AbortSignal,
  ): Promise<ResolvedCredential>;
}

/** Legacy/custom credential callback retained for source compatibility. */
export type CredentialCallback = (
  signal: AbortSignal,
) => Promise<string | undefined> | string | undefined;

export type CredentialProvider = AsyncCredentialProvider | CredentialCallback;

/** A secret-free provider failure safe to expose to the caller. */
export class KitaruCredentialError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "KitaruCredentialError";
  }
}

/** Create a fixed provider which never changes identity after rejection. */
export function createStaticCredentialProvider(
  token: string,
  identity = "explicit",
): AsyncCredentialProvider {
  if (token.length === 0) {
    throw new Error("Credential must not be empty");
  }
  const credential = { generation: 0, identity, token };
  return {
    getCredential: async () => credential,
  };
}

export function isAsyncCredentialProvider(
  provider: CredentialProvider,
): provider is AsyncCredentialProvider {
  return typeof provider !== "function";
}

export function isRenewableCredentialProvider(
  provider: CredentialProvider,
): provider is RenewableCredentialProvider {
  return (
    isAsyncCredentialProvider(provider) &&
    typeof (provider as Partial<RenewableCredentialProvider>)
      .renewCredential === "function"
  );
}

/** Freeze a custom provider to its first non-secret identity. */
export function bindCredentialProvider(
  provider: CredentialProvider,
): AsyncCredentialProvider | RenewableCredentialProvider {
  if (typeof provider === "function") {
    interface Resolution {
      controller: AbortController;
      pending: Promise<ResolvedCredential | undefined>;
      settled: boolean;
      waiters: number;
    }
    let resolution: Resolution | undefined;
    return {
      getCredential: async (signal) => {
        if (signal.aborted) {
          throw signal.reason;
        }
        let current = resolution;
        if (current === undefined) {
          const controller = new AbortController();
          const created: Resolution = {
            controller,
            pending: Promise.resolve(undefined),
            settled: false,
            waiters: 0,
          };
          current = created;
          resolution = created;
          created.pending = Promise.resolve()
            .then(() => provider(controller.signal))
            .then((token) =>
              token === undefined
                ? undefined
                : { generation: 0, identity: "custom", token },
            )
            .catch((error: unknown) => {
              if (resolution === created) {
                resolution = undefined;
              }
              throw error;
            })
            .finally(() => {
              created.settled = true;
            });
        }
        current.waiters += 1;
        try {
          return await awaitWithSignal(current.pending, signal);
        } finally {
          current.waiters -= 1;
          if (
            !current.settled &&
            current.waiters === 0 &&
            resolution === current
          ) {
            resolution = undefined;
            current.controller.abort(signal.reason);
          }
        }
      },
    };
  }

  let identity: string | undefined;
  const assertIdentity = (
    credential: ResolvedCredential | undefined,
  ): ResolvedCredential | undefined => {
    if (credential === undefined) {
      return undefined;
    }
    identity ??= credential.identity;
    if (credential.identity !== identity) {
      throw new KitaruCredentialError(
        "Credential provider changed identity; create a new client",
      );
    }
    return credential;
  };
  if (!isRenewableCredentialProvider(provider)) {
    return {
      getCredential: async (signal) =>
        assertIdentity(await provider.getCredential(signal)),
    };
  }
  return {
    getCredential: async (signal) =>
      assertIdentity(await provider.getCredential(signal)),
    renewCredential: async (rejected, signal) =>
      assertIdentity(
        await provider.renewCredential(rejected, signal),
      ) as ResolvedCredential,
  };
}
