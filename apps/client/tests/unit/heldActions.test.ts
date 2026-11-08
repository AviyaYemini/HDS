/**
 * Actions held for an undo (FR-046, R163): sent when the undo time is up, at once when the app
 * leaves the foreground or the page hides, before signing out, never after Undo and never after the
 * session has ended.
 */
import { AppState, Platform } from "react-native";

import { dropHeld, flushHeld, heldCount, hold, installHeldFlush } from "../../src/design/heldActions";
import { Session } from "../../src/api/session";
import { memoryStore } from "../../src/api/tokens";
import { BASE, FakeApi, json, signedIn } from "./fakeApi";

beforeEach(() => {
  jest.useFakeTimers();
  dropHeld();
});
afterEach(() => {
  jest.useRealTimers();
});

test("a held action is sent when its undo time is up, and not before", async () => {
  const send = jest.fn(async () => undefined);
  hold(send, 6000);
  await jest.advanceTimersByTimeAsync(5999);
  expect(send).not.toHaveBeenCalled();
  await jest.advanceTimersByTimeAsync(1);
  expect(send).toHaveBeenCalledWith(false);
  expect(heldCount()).toBe(0);
});

test("Undo in time sends nothing; too late, it says so", async () => {
  const send = jest.fn(async () => undefined);
  const undo = hold(send, 6000);
  expect(undo()).toBe(true);
  await jest.advanceTimersByTimeAsync(10_000);
  expect(send).not.toHaveBeenCalled();
  const late = hold(send, 6000);
  await flushHeld();
  expect(late()).toBe(false);
  expect(send).toHaveBeenCalledTimes(1);
});

test("leaving the foreground sends it at once, as urgent (keepalive)", async () => {
  let listener: ((state: string) => void) | null = null;
  const spy = jest.spyOn(AppState, "addEventListener").mockImplementation((_type, handler) => {
    listener = handler as (state: string) => void;
    return { remove: () => undefined } as ReturnType<typeof AppState.addEventListener>;
  });
  const uninstall = installHeldFlush();
  const send = jest.fn(async () => undefined);
  hold(send, 6000);
  listener!("background");
  await Promise.resolve();
  expect(send).toHaveBeenCalledWith(true);
  uninstall();
  spy.mockRestore();
});

test("the page hiding sends it at once on the web", async () => {
  const added = new Map<string, () => void>();
  const original = Platform.OS;
  Object.defineProperty(Platform, "OS", { value: "web", configurable: true });
  const fakeWindow = { addEventListener: (type: string, fn: () => void) => added.set(type, fn), removeEventListener: () => undefined };
  (globalThis as { window?: unknown }).window = fakeWindow;
  try {
    const uninstall = installHeldFlush();
    const send = jest.fn(async () => undefined);
    hold(send, 6000);
    added.get("pagehide")!();
    await Promise.resolve();
    expect(send).toHaveBeenCalledWith(true);
    uninstall();
  } finally {
    Object.defineProperty(Platform, "OS", { value: original, configurable: true });
    delete (globalThis as { window?: unknown }).window;
  }
});

test("signing out sends it first, while the session is still good", async () => {
  jest.useRealTimers();
  const order: string[] = [];
  const api = new FakeApi().on("POST", "/auth/logout", (seen) => {
    order.push(`logout ${seen.auth ?? ""}`);
    return json(204, {});
  });
  const session: Session = await signedIn(api, memoryStore());
  session.beforeSignOut(() => flushHeld());
  hold(async () => {
    order.push(`delete signed in: ${session.signedIn}`);
  }, 6000);
  await session.signOut();
  expect(order[0]).toBe("delete signed in: true");
  expect(order[1]).toMatch(/^logout Bearer/);
  void BASE;
});

test("once the session has ended, held actions are dropped unsent", async () => {
  const send = jest.fn(async () => undefined);
  hold(send, 6000);
  dropHeld();
  await jest.advanceTimersByTimeAsync(10_000);
  expect(send).not.toHaveBeenCalled();
});
