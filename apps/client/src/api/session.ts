/**
 * The signed-in session: signing in and out, the caller's account, and the typed API client that
 * sends the access token and, on a 401, refreshes the tokens once and sends the request again.
 */
import { createApiClient, type ApiClient, type components } from "@hds/api-client";
import { Platform } from "react-native";

import { ApiError, call, codeOf } from "./errors";
import type { TokenStore, Tokens } from "./tokens";

/** The signed-in account, with what it may do (specs/007-ux-redesign FR-001). */
export type Me = components["schemas"]["MeRead"];

/**
 * What is signing in: the phone app gets a 30-day refresh token that renews on use, the web keeps
 * 7 days (specs/007-ux-redesign FR-010, ADR 016).
 */
export const CLIENT_KIND: "web" | "native" = Platform.OS === "web" ? "web" : "native";

type Send = (request: Request) => Promise<Response>;

/** One signed-in user's tokens and an API client that uses them. */
export class Session {
  /** The API client for signed-in requests. */
  readonly api: ApiClient;
  /** The account, once signed in. */
  user: Me | null = null;

  private readonly plain: ApiClient;
  private tokens: Tokens | null = null;
  private refreshing: Promise<boolean> | null = null;
  private readonly listeners = new Set<() => void>();

  constructor(
    baseUrl: string,
    private readonly store: TokenStore,
    private readonly send: Send = (request) => fetch(request),
  ) {
    this.plain = createApiClient({ baseUrl, fetch: send });
    this.api = createApiClient({ baseUrl, fetch: (request) => this.authorised(request) });
  }

  /** Whether someone is signed in. */
  get signedIn(): boolean {
    return this.tokens !== null && this.user !== null;
  }

  /** Call `listener` when someone signs in or the session ends; returns the unsubscribe. */
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  /** Take up kept tokens, if any still work; an unreachable server keeps them for the next try. */
  async restore(): Promise<boolean> {
    this.tokens = await this.store.load();
    if (this.tokens === null) return false;
    try {
      await this.loadMe();
    } catch (error) {
      if (error instanceof ApiError && error.status === 0) {
        this.tokens = null;
        return false;
      }
      await this.forget(false);
      return false;
    }
    this.notify();
    return true;
  }

  /** Sign in; answers `null`, or the code of the refusal. */
  async signIn(email: string, password: string): Promise<string | null> {
    try {
      const answer = await call(this.plain.POST("/auth/login", { body: { email, password, client_kind: CLIENT_KIND } }));
      await this.keep({ access: answer.access_token, refresh: answer.refresh_token });
      await this.loadMe();
    } catch (error) {
      await this.forget(false);
      return codeOf(error);
    }
    this.notify();
    return null;
  }

  /**
   * Revoke the refresh token if the API can be reached, and forget the tokens either way. An expired
   * access token is refreshed first and the logout sent again with the refresh token it was rotated
   * to, so the session the server still holds is the one revoked.
   */
  async signOut(): Promise<void> {
    try {
      let status = await this.logout();
      if (status === 401 && (await this.refresh())) status = await this.logout();
    } catch {
      // Unreachable: the tokens are forgotten here and expire on the server.
    }
    await this.forget(true);
  }

  /** Send a logout with the current tokens; answers its status, or null when signed out. */
  private async logout(): Promise<number | null> {
    const tokens = this.tokens;
    if (tokens === null) return null;
    const { response } = await this.plain.POST("/auth/logout", {
      body: { refresh_token: tokens.refresh },
      headers: { Authorization: `Bearer ${tokens.access}` },
    });
    return response.status;
  }

  private async loadMe(): Promise<void> {
    this.user = await call(this.api.GET("/me"));
  }

  private async keep(tokens: Tokens): Promise<void> {
    this.tokens = tokens;
    await this.store.save(tokens);
  }

  private async forget(notify: boolean): Promise<void> {
    const was = this.signedIn;
    this.tokens = null;
    this.user = null;
    await this.store.clear();
    if (notify && was) this.notify();
  }

  private notify(): void {
    for (const listener of this.listeners) listener();
  }

  private withToken(request: Request): Request {
    const sent = new Request(request);
    if (this.tokens !== null) sent.headers.set("Authorization", `Bearer ${this.tokens.access}`);
    return sent;
  }

  private async authorised(request: Request): Promise<Response> {
    const spare = request.clone();
    const first = await this.send(this.withToken(request));
    if (first.status !== 401 || this.tokens === null) return first;
    if (!(await this.refresh())) return first;
    return this.send(this.withToken(spare));
  }

  /** Refresh the tokens; requests refused together share one refresh. */
  private refresh(): Promise<boolean> {
    this.refreshing ??= this.renew().finally(() => {
      this.refreshing = null;
    });
    return this.refreshing;
  }

  private async renew(): Promise<boolean> {
    const refresh = this.tokens?.refresh;
    if (refresh === undefined) return false;
    try {
      const answer = await call(this.plain.POST("/auth/refresh", { body: { refresh_token: refresh } }));
      await this.keep({ access: answer.access_token, refresh: answer.refresh_token });
      return true;
    } catch (error) {
      if (error instanceof ApiError && error.status === 0) return false;
      await this.forget(true);
      return false;
    }
  }
}
