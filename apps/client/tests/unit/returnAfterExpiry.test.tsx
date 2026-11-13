/**
 * A session that ends on a page with a dynamic segment comes back to that page (the owner,
 * 2026-11-13): the router first rewrites the address to the section's first route, keeping the old
 * page's params, so the shell remembers where it was and the sign-in redirect prefers that.
 */
import { render, screen } from "@testing-library/react-native";
import * as Router from "expo-router";

import { SignInRedirect, forgetLocation, useRememberLocation } from "../../src/auth/SignInRedirect";

const router = Router as unknown as {
  __setPathname: (p: string) => void;
  __setSearchParams: (p: Record<string, string>) => void;
  __setSegments: (s: string[]) => void;
};

function Remember() {
  useRememberLocation();
  return null;
}

afterEach(() => {
  forgetLocation();
  router.__setPathname("/");
  router.__setSearchParams({});
  router.__setSegments([]);
});

test("an ended session on /runs/<id> returns there, not to /home?id=<id>", async () => {
  router.__setPathname("/runs/abc");
  router.__setSearchParams({ id: "abc" });
  router.__setSegments(["(staff)", "runs", "[id]"]);
  await render(<Remember />);
  // The router has moved to the section's first route, keeping the old page's params.
  router.__setPathname("/home");
  router.__setSegments(["(staff)", "home"]);
  await render(<SignInRedirect />);
  expect(screen.getByTestId("redirect")).toHaveTextContent(`/sign-in?next=${encodeURIComponent("/runs/abc")}`);
});

test("after a chosen sign-out, the address in hand is used", async () => {
  router.__setPathname("/my/shifts/xyz");
  router.__setSearchParams({ id: "xyz" });
  router.__setSegments(["(staff)", "my", "shifts", "[id]"]);
  await render(<Remember />);
  forgetLocation();
  await render(<SignInRedirect />);
  expect(screen.getByTestId("redirect")).toHaveTextContent(`/sign-in?next=${encodeURIComponent("/my/shifts/xyz")}`);
});
