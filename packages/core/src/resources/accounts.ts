import type { KitaruTransport } from "../transport.js";
import type { AccountResponse } from "../types.js";
import { validateAccount } from "./internal.js";
import type { ResourceRequestOptions } from "./pagination.js";

export class AccountsResource {
  readonly #transport: KitaruTransport;

  constructor(transport: KitaruTransport) {
    this.#transport = transport;
  }

  async getCurrent(
    options: ResourceRequestOptions = {},
  ): Promise<AccountResponse> {
    return this.#transport.request({
      method: "GET",
      path: "/api/v1/accounts/me",
      signal: options.signal,
      validate: validateAccount,
    });
  }
}
