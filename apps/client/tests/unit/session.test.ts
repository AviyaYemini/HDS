// Session: where tokens are kept (R9), signing in and out, and one refresh on a 401.
import * as SecureStore from "expo-secure-store";

import { Session } from "../../src/api/session";
import { memoryStore, storeFor } from "../../src/api/tokens";
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

test("on the web the tokens stay in memory only, so a reload signs in again", async () => {
  const api = new FakeApi();
  const local = { setItem: jest.fn(), getItem: jest.fn(), removeItem: jest.fn() };
  Object.defineProperty(globalThis, "localStorage", { value: local, configurable: true });
  Object.defineProperty(globalThis, "sessionStorage", { value: local, configurable: true });
  try {
    await signedIn(api, storeFor("web"));
  } finally {
    delete (globalThis as { localStorage?: unknown }).localStorage;
    delete (globalThis as { sessionStorage?: unknown }).sessionStorage;
  }
  expect(secure.setItemAsync).not.toHaveBeenCalled();
  expect(local.setItem).not.toHaveBeenCalled();

  const reloaded = new Session(BASE, storeFor("web"), api.fetch);
  expect(await reloaded.restore()).toBe(false);
  expect(reloaded.signedIn).toBe(false);
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
});

test("an unreachable server on restore keeps the stored tokens for the next try", async () => {
  const store = memoryStore();
  await store.save({ access: "a1", refresh: "r1" });
  const offline = new Session(BASE, store, async () => {
    throw new TypeError("Network request failed");
  });
  expect(await offline.restore()).toBe(false);
  expect(await store.load()).toEqual({ access: "a1", refresh: "r1" });
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
