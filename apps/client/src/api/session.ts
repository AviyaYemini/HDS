/**
 * The signed-in session: signing in and out, the caller's account, and the typed API client that
 * sends the access token and, on a 401, refreshes the tokens once and sends the request again. After
 * any 403 the account is read again, so a role or project changed elsewhere redraws what the
 * person may do (specs/007-ux-redesign FR-001, T032).
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

/** What `send` is told about a request: a background one never shows the loading bar. */
export interface SendOptions {
  background?: boolean;
}

type Send = (request: Request, options?: SendOptions) => Promise<Response>;

/** One signed-in user's tokens and an API client that uses them. */
export class Session {
  /** The API client for signed-in requests. */
  readonly api: ApiClient;
  /** The same, for background requests (a poll) that never show the loading bar (FR-074). */
  readonly quiet: ApiClient;
  /**
   * Whether the last {@link restore} found kept tokens but could not reach the server. The tokens
   * stay kept; the start screen shows Retry instead of the sign-in form (PR 2 review).
   */
  unreachable = false;
  /** The account, once signed in. */
  user: Me | null = null;
  /** True after the server refused to renew the session, until the next sign-in (FR-010's notice). */
  endedUnexpectedly = false;

  private readonly plain: ApiClient;
  /** Unsigned and in the background: the token refresh. */
  private readonly plainQuiet: ApiClient;
  private tokens: Tokens | null = null;
  private refreshing: Promise<boolean> | null = null;
  private reloading: Promise<void> | null = null;
  private readonly listeners = new Set<() => void>();
  private readonly signOutTasks = new Set<() => Promise<void>>();

  constructor(
    baseUrl: string,
    private readonly store: TokenStore,
    private readonly send: Send = (request) => fetch(request),
  ) {
    this.plain = createApiClient({ baseUrl, fetch: (request) => send(request) });
    this.plainQuiet = createApiClient({ baseUrl, fetch: (request) => send(request, { background: true }) });
    this.api = createApiClient({ baseUrl, fetch: (request) => this.authorised(request) });
    this.quiet = createApiClient({ baseUrl, fetch: (request) => this.authorised(request, { background: true }) });
  }

  /** Whether someone is signed in. */
  get signedIn(): boolean {
    return this.tokens !== null && this.user !== null;
  }

  /**
   * Call `listener` when someone signs in, the session ends or the account is read again; returns
   * the unsubscribe.
   */
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  /** Take up kept tokens, if any still work; an unreachable server keeps them for the next try. */
  async restore(): Promise<boolean> {
    this.unreachable = false;
    this.tokens = await this.store.load();
    if (this.tokens === null) return false;
    try {
      await this.loadMe();
    } catch (error) {
      if (error instanceof ApiError && error.status === 0) {
        // Kept, in the store and for the next try; the start screen offers Retry, not sign-in.
        this.tokens = null;
        this.unreachable = true;
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
    // Tokens kept from an earlier session (say, a start with no network, or the previous guard
    // on a shared phone) are replaced below, so that session is revoked first, with its own
    // credentials, rather than left live on the server.
    const earlier = this.tokens ?? (await this.store.load());
    try {
      const answer = await call(this.plain.POST("/auth/login", { body: { email, password, client_kind: CLIENT_KIND } }));
      if (earlier !== null && earlier.refresh !== answer.refresh_token) await this.revokeKept(earlier);
      await this.keep({ access: answer.access_token, refresh: answer.refresh_token });
      await this.loadMe();
      this.unreachable = false;
    } catch (error) {
      await this.forget(false);
      return codeOf(error);
    }
    this.endedUnexpectedly = false;
    this.notify();
    return null;
  }

  /**
   * Revoke the refresh token if the API can be reached, and forget the tokens either way. An expired
   * access token is refreshed first and the logout sent again with the refresh token it was rotated
   * to, so the session the server still holds is the one revoked.
   */
  /**
   * Run `task` before every sign-out, while the session is still good (held deletes are sent then,
   * R163); returns the removal.
   */
  beforeSignOut(task: () => Promise<void>): () => void {
    this.signOutTasks.add(task);
    return () => {
      this.signOutTasks.delete(task);
    };
  }

  async signOut(): Promise<void> {
    for (const task of [...this.signOutTasks]) {
      try {
        await task();
      } catch {
        // A task that fails never keeps someone signed in.
      }
    }
    // Signed out from a lock screen, the kept tokens were never taken up: take them now, so the
    // server revokes the session rather than only this device forgetting it.
    if (this.tokens === null) this.tokens = await this.store.load();
    try {
      let status = await this.logout();
      if (status === 401 && (await this.refresh())) status = await this.logout();
    } catch {
      // Unreachable: the tokens are forgotten here and expire on the server.
    }
    await this.forget(true);
  }

  /**
   * Revoke a kept session with its own credentials, as signing out does: the logout is refused for
   * another person's token on purpose (FR-238), so the new sign-in's token cannot do it. An expired
   * access token is refreshed once first. Best effort: a failure leaves the session to expire and
   * never fails the sign-in.
   */
  private async revokeKept(kept: Tokens): Promise<void> {
    try {
      if ((await this.logoutWith(kept)) !== 401) return;
      const renewed = await this.plain.POST("/auth/refresh", { body: { refresh_token: kept.refresh } });
      if (!renewed.response.ok || renewed.data === undefined) return;
      await this.logoutWith({ access: renewed.data.access_token, refresh: renewed.data.refresh_token });
    } catch {
      // Unreachable: it expires on the server.
    }
  }

  /** Send a logout with the current tokens; answers its status, or null when signed out. */
  private async logout(): Promise<number | null> {
    return this.tokens === null ? null : this.logoutWith(this.tokens);
  }

  /** Send a logout for `tokens`; answers its status. */
  private async logoutWith(tokens: Tokens): Promise<number> {
    const { response } = await this.plain.POST("/auth/logout", {
      body: { refresh_token: tokens.refresh },
      headers: { Authorization: `Bearer ${tokens.access}` },
    });
    return response.status;
  }

  private async loadMe(): Promise<void> {
    this.user = await call(this.api.GET("/me"));
  }

  /**
   * Read the account again after a refusal; refusals together share one read. A failed read keeps
   * the account as it was (a 401 that cannot refresh has already ended the session).
   */
  reloadMe(): Promise<void> {
    this.reloading ??= (async () => {
      try {
        const before = JSON.stringify(this.user);
        await this.loadMe();
        // Compared by value: only a real change (role, capabilities, projects) redraws.
        if (JSON.stringify(this.user) !== before) this.notify();
      } catch {
        // Unreachable or refused: the next refusal tries again.
      } finally {
        this.reloading = null;
      }
    })();
    return this.reloading;
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

  private async authorised(request: Request, options: SendOptions = {}): Promise<Response> {
    const spare = request.clone();
    const first = await this.send(this.withToken(request), options);
    if (first.status === 403) this.refusedBy(request);
    if (first.status !== 401 || this.tokens === null) return first;
    if (!(await this.refresh())) return first;
    const second = await this.send(this.withToken(spare), options);
    if (second.status === 403) this.refusedBy(spare);
    return second;
  }

  /** A 403 may mean the role changed: read the account again, unless `/me` itself was refused. */
  private refusedBy(request: Request): void {
    if (new URL(request.url).pathname.endsWith("/me")) return;
    void this.reloadMe();
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
      const answer = await call(this.plainQuiet.POST("/auth/refresh", { body: { refresh_token: refresh } }));
      await this.keep({ access: answer.access_token, refresh: answer.refresh_token });
      return true;
    } catch (error) {
      if (error instanceof ApiError && error.status === 0) return false;
      this.endedUnexpectedly = true;
      await this.forget(true);
      return false;
    }
  }
}
