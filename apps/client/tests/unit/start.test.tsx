/**
 * Starting with kept tokens and no server (PR 2 review): the "can't reach HDS" state with Retry and
 * "Sign in again", never the sign-in form, and a retry on its own when the app comes back.
 */
import { fireEvent, render, screen } from "@testing-library/react-native";
import { AppState } from "react-native";

import { useRetryOnReturn } from "../../src/auth/retryOnReturn";
import { startAfterRestore } from "../../src/auth/startState";
import { UnreachableScreen } from "../../src/screens/misc/UnreachableScreen";

describe("startAfterRestore", () => {
  it.each([
    [false, true, "unreachable", "kept tokens, server out of reach"],
    [false, false, "ready", "nothing kept, or tokens refused (the routes send to sign-in)"],
    [true, false, "ready", "a restored session"],
  ])("restored=%s unreachable=%s → %s (%s)", (restored, unreachable, expected) => {
    expect(startAfterRestore(restored, unreachable)).toBe(expected);
  });
});

describe("UnreachableScreen", () => {
  it("offers Retry first and signing in again second", async () => {
    const onRetry = jest.fn();
    const onSignIn = jest.fn();
    await render(<UnreachableScreen onRetry={onRetry} onSignIn={onSignIn} busy={false} />);
    expect(screen.getByTestId("screen-unreachable")).toBeTruthy();
    expect(screen.getByText("Can't reach HDS")).toBeTruthy();
    await fireEvent.press(screen.getByTestId("action-retry"));
    expect(onRetry).toHaveBeenCalledTimes(1);
    await fireEvent.press(screen.getByTestId("action-sign-in-again"));
    expect(onSignIn).toHaveBeenCalledTimes(1);
    expect(screen.queryByTestId("sign-in-btn")).toBeNull();
  });
});

describe("useRetryOnReturn", () => {
  function Probe({ active, retry }: { active: boolean; retry: () => void }) {
    useRetryOnReturn(active, retry);
    return null;
  }

  it("retries when the app returns to the foreground, only while waiting", async () => {
    const remove = jest.fn();
    let handler: ((state: string) => void) | null = null;
    const spy = jest.spyOn(AppState, "addEventListener").mockImplementation((_type, listener) => {
      handler = listener as (state: string) => void;
      return { remove } as never;
    });
    const retry = jest.fn();
    const view = await render(<Probe active={false} retry={retry} />);
    expect(spy).not.toHaveBeenCalled();
    await view.rerender(<Probe active retry={retry} />);
    expect(spy).toHaveBeenCalledTimes(1);
    handler!("background");
    expect(retry).not.toHaveBeenCalled();
    handler!("active");
    expect(retry).toHaveBeenCalledTimes(1);
    await view.rerender(<Probe active={false} retry={retry} />);
    expect(remove).toHaveBeenCalled();
    spy.mockRestore();
  });
});
