/**
 * The HDS API client: generated types (`schema.d.ts`) and a typed `openapi-fetch` client over them.
 *
 * No request is written by hand (Constitution V): paths, bodies and answers are checked against
 * the OpenAPI document at compile time. Regenerate with `pnpm api-client:generate`.
 */
import createClient, { type Client, type Middleware } from "openapi-fetch";

import type { components, paths } from "./schema";

export type { components, paths };
export type ApiClient = Client<paths>;
export type { Middleware };

/** A problem answer from the API: `code` is the stable key a client translates. */
export type Problem = components["schemas"]["ProblemDetail"];

/** Options for {@link createApiClient}. */
export interface ApiClientOptions {
  /** The API's origin, for example `http://localhost:8000`. */
  baseUrl: string;
  /** The fetch to send requests with; defaults to the global one. */
  fetch?: (request: Request) => Promise<Response>;
  /** Middleware run on every request and answer, in order. */
  middleware?: Middleware[];
}

/** A typed client for the API at `options.baseUrl`. */
export function createApiClient(options: ApiClientOptions): ApiClient {
  const client = createClient<paths>({ baseUrl: options.baseUrl, fetch: options.fetch });
  for (const m of options.middleware ?? []) client.use(m);
  return client;
}
