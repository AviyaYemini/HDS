// Session: where tokens are kept (R9), signing in and out, and one refresh on a 401.
import * as SecureStore from "expo-secure-store";

import { Session } from "../../src/api/session";
import { memoryStore, resetWebStoreForTests, storeFor, tabStore } from "../../src/api/tokens";
import { BASE, FakeApi, ME, TOKENS, json, problem, signedIn } from "./fakeApi";

const secure = SecureStore as unknown as {
  __items: Map<string, string>;
  setItemAsync: jest.Mock;
  getItemAsync: jest.Mock;
};

beforeEach(() => {
  secure.__items.clear();
  jest.clearAllMocks();
});

test("on a phone the tokens go to the secure store and come back on restore", async () => {
  const api = new FakeApi();
  await signedIn(api, storeFor("ios"));
  expect(secure.setItemAsync).toHaveBeenCalled();
  expect([...secure.__items.values()].join()).toContain("r1");

  const again = new Session(BASE, storeFor("ios"), api.fetch);
  expect(await again.restore()).toBe(true);
  expect(again.user?.id).toBe(ME.id);
});

/** A tab's sessionStorage and its page events, for the tab store (ADR 019). */
function tab(copy: Record<string, string> = {}) {
  const items = new Map(Object.entries(copy));
  const listeners: Record<string, (() => void)[]> = {};
  return {
    items,
    storage: {
      getItem: (k: string) => items.get(k) ?? null,
      setItem: (k: string, v: string) => void items.set(k, v),
      removeItem: (k: string) => void items.delete(k),
    },
    page: {
      addEventListener: (type: string, listener: () => void) => {
        (listeners[type] ??= []).push(listener);
      },
    },
    fire: (type: string) => (listeners[type] ?? []).forEach((l) => l()),
  };
}

test("on the web the tokens are kept per tab: a reload in the same tab stays signed in", async () => {
  const api = new FakeApi();
  const first = tab();
  await signedIn(api, tabStore(first.storage, first.page));
  expect(secure.setItemAsync).not.toHaveBeenCalled();
  // While the page runs the tab holds nothing a duplicate could copy.
  expect(first.items.size).toBe(0);
  first.fire("pagehide");
  expect(first.items.size).toBe(1);

  // The reload: a new page in the same tab takes the copy and removes it at once.
  const reloaded = new Session(BASE, tabStore(first.storage, first.page), api.fetch);
  expect(await reloaded.restore()).toBe(true);
  expect(first.items.size).toBe(0);
});

test("a tab duplicated while the page is open finds nothing and signs in again", async () => {
  const api = new FakeApi();
  const original = tab();
  await signedIn(api, tabStore(original.storage, original.page));
  // The browser copies the tab's storage into the duplicate: there is nothing in it.
  const duplicate = tab(Object.fromEntries(original.items));
  const second = new Session(BASE, tabStore(duplicate.storage, duplicate.page), api.fetch);
  expect(await second.restore()).toBe(false);
  expect(api.calls("POST", "/auth/refresh")).toHaveLength(0);
});

test("a page restored from the back-forward cache takes the tab's copy away again", async () => {
  const api = new FakeApi();
  const page = tab();
  await signedIn(api, tabStore(page.storage, page.page));
  page.fire("pagehide");
  expect(page.items.size).toBe(1);
  page.fire("pageshow");
  expect(page.items.size).toBe(0);
});

test("signing out clears the tab, and blocked storage only means signing in again", async () => {
  const api = new FakeApi();
  api.on("POST", "/auth/logout", new Response(null, { status: 204 }));
  const page = tab();
  const session = await signedIn(api, tabStore(page.storage, page.page));
  await session.signOut();
  page.fire("pagehide");
  expect(page.items.size).toBe(0);

  const blocked = {
    getItem: () => {
      throw new Error("blocked");
    },
    setItem: () => {
      throw new Error("blocked");
    },
    removeItem: () => {
      throw new Error("blocked");
    },
  };
  const store = tabStore(blocked, null);
  await store.save({ access: "a", refresh: "r" });
  expect(await store.load()).toEqual({ access: "a", refresh: "r" });
});

test("there is one web store per page, so a second look at it does not take the tokens", () => {
  resetWebStoreForTests();
  expect(storeFor("web")).toBe(storeFor("web"));
});

test("requests carry the access token", async () => {
  const api = new FakeApi().on("GET", "/me/shifts", json(200, { items: [] }));
  const session = await signedIn(api);
  await session.api.GET("/me/shifts");
  expect(api.calls("GET", "/me/shifts")[0]!.auth).toBe("Bearer a1");
});

test("a 401 refreshes once and the request is sent again with the new token", async () => {
  const api = new FakeApi()
    .on("GET", "/me/shifts", problem(401, "auth.token_expired"), json(200, { items: [] }))
    .on("POST", "/auth/refresh", json(200, { ...TOKENS, access_token: "a2", refresh_token: "r2" }));
  const session = await signedIn(api);
  const { data } = await session.api.GET("/me/shifts");
  expect(data).toEqual({ items: [] });
  expect(api.calls("POST", "/auth/refresh")).toHaveLength(1);
  expect(api.calls("POST", "/auth/refresh")[0]!.body).toEqual({ refresh_token: "r1" });
  expect(api.calls("GET", "/me/shifts").map((c) => c.auth)).toEqual(["Bearer a1", "Bearer a2"]);
});

test("the token refresh and quiet requests are sent in the background; others are not", async () => {
  const api = new FakeApi()
    .on("GET", "/me/shifts", problem(401, "auth.token_expired"), json(200, { items: [] }), json(200, { items: [] }))
    .on("POST", "/auth/refresh", json(200, { ...TOKENS, access_token: "a2", refresh_token: "r2" }));
  const seen: [string, boolean][] = [];
  const store = memoryStore();
  await signedIn(api, store);
  const session = new Session(BASE, store, (request, options) => {
    seen.push([new URL(request.url).pathname, options?.background === true]);
    return api.fetch(request);
  });
  await session.restore();
  seen.length = 0;
  await session.api.GET("/me/shifts");
  await session.quiet.GET("/me/shifts");
  expect(seen).toEqual([
    ["/me/shifts", false],
    ["/auth/refresh", true],
    ["/me/shifts", false],
    ["/me/shifts", true],
  ]);
});

test("a second 401 after the refresh is returned as it is, with no second refresh", async () => {
  const api = new FakeApi()
    .on("GET", "/me/shifts", problem(401, "auth.token_revoked"))
    .on("POST", "/auth/refresh", json(200, { ...TOKENS, access_token: "a2", refresh_token: "r2" }));
  const session = await signedIn(api);
  const { response } = await session.api.GET("/me/shifts");
  expect(response.status).toBe(401);
  expect(api.calls("POST", "/auth/refresh")).toHaveLength(1);
});

test("two requests refused together share one refresh", async () => {
  const api = new FakeApi()
    .on("GET", "/me/shifts", problem(401, "auth.token_expired"), problem(401, "auth.token_expired"), json(200, { items: [] }))
    .on("POST", "/auth/refresh", json(200, { ...TOKENS, access_token: "a2", refresh_token: "r2" }));
  const session = await signedIn(api);
  await Promise.all([session.api.GET("/me/shifts"), session.api.GET("/me/shifts")]);
  expect(api.calls("POST", "/auth/refresh")).toHaveLength(1);
});

test("a refused refresh signs the session out and tells the listeners", async () => {
  const api = new FakeApi()
    .on("GET", "/me/shifts", problem(401, "auth.token_expired"))
    .on("POST", "/auth/refresh", problem(401, "auth.token_used"));
  const session = await signedIn(api);
  const out = jest.fn();
  session.subscribe(out);
  await session.api.GET("/me/shifts");
  expect(session.signedIn).toBe(false);
  expect(out).toHaveBeenCalled();
});

test("a refused sign-in answers the API's code and keeps nothing", async () => {
  const api = new FakeApi().on("POST", "/auth/login", problem(401, "auth.invalid_credentials"));
  const store = memoryStore();
  const session = new Session(BASE, store, api.fetch);
  expect(await session.signIn("a@b.c", "wrong")).toBe("auth.invalid_credentials");
  expect(session.signedIn).toBe(false);
  expect(await store.load()).toBeNull();
});

test("an unreachable server answers errors.network", async () => {
  const session = new Session(BASE, memoryStore(), async () => {
    throw new TypeError("Network request failed");
  });
  expect(await session.signIn("a@b.c", "x")).toBe("errors.network");
});

test("an answer with no code is errors.unknown", async () => {
  const api = new FakeApi().on("POST", "/auth/login", new Response("oops", { status: 500 }));
  const session = new Session(BASE, memoryStore(), api.fetch);
  expect(await session.signIn("a@b.c", "x")).toBe("errors.unknown");
});

test("signing out revokes the refresh token and forgets the tokens", async () => {
  const api = new FakeApi().on("POST", "/auth/logout", new Response(null, { status: 204 }));
  const store = memoryStore();
  const session = await signedIn(api, store);
  await session.signOut();
  expect(api.calls("POST", "/auth/logout")[0]).toMatchObject({
    auth: "Bearer a1",
    body: { refresh_token: "r1" },
  });
  expect(session.signedIn).toBe(false);
  expect(await store.load()).toBeNull();
});

test("signing out still forgets the tokens when the server cannot be reached", async () => {
  const api = new FakeApi();
  const session = await signedIn(api);
  api.on("POST", "/auth/logout", () => {
    throw new TypeError("Network request failed");
  });
  await session.signOut();
  expect(session.signedIn).toBe(false);
});

test("a stored session whose tokens no longer work is forgotten on restore", async () => {
  const store = memoryStore();
  await store.save({ access: "old", refresh: "old" });
  const api = new FakeApi()
    .on("GET", "/me", problem(401, "auth.token_expired"))
    .on("POST", "/auth/refresh", problem(401, "auth.token_revoked"));
  const session = new Session(BASE, store, api.fetch);
  expect(await session.restore()).toBe(false);
  expect(await store.load()).toBeNull();
  // The server refused the kept tokens: that is a session that ended (R25).
  expect(session.endedUnexpectedly).toBe(true);
});

test("an unreachable server on restore keeps the stored tokens for the next try", async () => {
  const store = memoryStore();
  await store.save({ access: "a1", refresh: "r1" });
  const offline = new Session(BASE, store, async () => {
    throw new TypeError("Network request failed");
  });
  expect(await offline.restore()).toBe(false);
  expect(await store.load()).toEqual({ access: "a1", refresh: "r1" });
  // Out of reach is not ended: no "session expired" notice for an offline start (R25).
  expect(offline.endedUnexpectedly).toBe(false);
});

test("signing out with an expired access token revokes the refresh token it was rotated to", async () => {
  const api = new FakeApi()
    .on("POST", "/auth/logout", problem(401, "auth.token_expired"), new Response(null, { status: 204 }))
    .on("POST", "/auth/refresh", json(200, { ...TOKENS, access_token: "a2", refresh_token: "r2" }));
  const session = await signedIn(api);
  await session.signOut();
  expect(api.calls("POST", "/auth/logout").map((c) => c.body)).toEqual([
    { refresh_token: "r1" },
    { refresh_token: "r2" },
  ]);
  expect(api.calls("POST", "/auth/logout")[1]!.auth).toBe("Bearer a2");
  expect(session.signedIn).toBe(false);
});

test("signing out before the kept tokens were taken up still revokes them on the server", async () => {
  // A phone locked behind biometrics signs out without ever restoring (007 review).
  const api = new FakeApi();
  const store = memoryStore();
  await store.save({ access: "a-kept", refresh: "r-kept" });
  api.on("POST", "/auth/logout", new Response(null, { status: 204 }));
  const session = new Session(BASE, store, api.fetch);
  await session.signOut();
  const [sent] = api.calls("POST", "/auth/logout");
  expect(sent?.body).toEqual({ refresh_token: "r-kept" });
  expect(sent?.auth).toBe("Bearer a-kept");
  expect(await store.load()).toBeNull();
});

test("starting with no network keeps the kept tokens and says the server was unreachable", async () => {
  // PR 2 review: a phone opened offline must not fall to the sign-in form (it shows Retry instead).
  const api = new FakeApi();
  const store = memoryStore();
  await store.save({ access: "a-kept", refresh: "r-kept" });
  const offline = new Session(BASE, store, async () => {
    throw new TypeError("network down");
  });
  expect(await offline.restore()).toBe(false);
  expect(offline.unreachable).toBe(true);
  expect(await store.load()).toEqual({ access: "a-kept", refresh: "r-kept" });

  // Back online, the same session restores and the flag clears.
  api.on("GET", "/me", json(200, ME));
  const online = new Session(BASE, store, api.fetch);
  expect(await online.restore()).toBe(true);
  expect(online.unreachable).toBe(false);
});

test("a refused restore is not reported as unreachable", async () => {
  const api = new FakeApi();
  const store = memoryStore();
  await store.save({ access: "a-old", refresh: "r-old" });
  api.on("GET", "/me", problem(401, "auth.token_expired"));
  api.on("POST", "/auth/refresh", problem(401, "auth.token_revoked"));
  const session = new Session(BASE, store, api.fetch);
  expect(await session.restore()).toBe(false);
  expect(session.unreachable).toBe(false);
});

test("signing in over kept tokens revokes the earlier session with its own credentials", async () => {
  // PR 2 review: otherwise the earlier 30-day refresh token stays live as a phantom device.
  const api = new FakeApi();
  const store = memoryStore();
  await store.save({ access: "a-old", refresh: "r-old" });
  api.on("POST", "/auth/logout", new Response(null, { status: 204 }));
  const session = await signedIn(api, store);
  const [sent] = api.calls("POST", "/auth/logout");
  expect(sent?.body).toEqual({ refresh_token: "r-old" });
  // Its own access token: the logout ignores another person's token on purpose (FR-238).
  expect(sent?.auth).toBe("Bearer a-old");
  expect(session.signedIn).toBe(true);
  expect(await store.load()).toEqual({ access: TOKENS.access_token, refresh: TOKENS.refresh_token });
});

test("on a shared phone the previous person's expired session is refreshed once, then revoked", async () => {
  // A different guard signs in: the old access token has expired (401), so the old refresh token
  // is exchanged once and the rotated pair is logged out, before the new tokens are kept.
  const api = new FakeApi();
  const store = memoryStore();
  await store.save({ access: "a-guard1", refresh: "r-guard1" });
  api.on(
    "POST",
    "/auth/logout",
    problem(401, "auth.token_expired"),
    new Response(null, { status: 204 }),
  );
  api.on("POST", "/auth/refresh", json(200, { ...TOKENS, access_token: "a-guard1b", refresh_token: "r-guard1b" }));
  api.on("GET", "/me", json(200, { ...ME, id: "99999999-9999-4999-8999-999999999999", email: "extra@hds.test" }));
  const session = await signedIn(api, store);
  expect(api.calls("POST", "/auth/refresh").map((c) => c.body)).toEqual([{ refresh_token: "r-guard1" }]);
  const logouts = api.calls("POST", "/auth/logout");
  expect(logouts.map((c) => [c.auth, c.body])).toEqual([
    ["Bearer a-guard1", { refresh_token: "r-guard1" }],
    ["Bearer a-guard1b", { refresh_token: "r-guard1b" }],
  ]);
  expect(session.user?.email).toBe("extra@hds.test");
  expect(await store.load()).toEqual({ access: TOKENS.access_token, refresh: TOKENS.refresh_token });
});

test("a failed revoke of the earlier session never fails the sign-in", async () => {
  const api = new FakeApi();
  const store = memoryStore();
  await store.save({ access: "a-old", refresh: "r-old" });
  api.on("POST", "/auth/logout", problem(500, "errors.unknown"));
  const session = await signedIn(api, store);
  expect(session.signedIn).toBe(true);
});
