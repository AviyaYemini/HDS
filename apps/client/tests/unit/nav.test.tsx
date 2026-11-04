// The employee bar (003 US4, US7; 001 US1 sign-out): moving between the screens, and signing out.
import { fireEvent, screen, waitFor } from "@testing-library/react-native";
import * as Router from "expo-router";

import { EmployeeNav } from "../../src/components/EmployeeNav";
import { FakeApi, renderWith, signedIn } from "./fakeApi";

const router = (Router as unknown as { __router: { replace: jest.Mock; push: jest.Mock } }).__router;

beforeEach(() => jest.clearAllMocks());

test("moves between My availability and My shifts", async () => {
  await renderWith(await signedIn(new FakeApi()), <EmployeeNav />);
  await fireEvent.press(screen.getByTestId("nav-shifts"));
  expect(router.push).toHaveBeenLastCalledWith("/shifts");
  await fireEvent.press(screen.getByTestId("nav-availability"));
  expect(router.push).toHaveBeenLastCalledWith("/availability");
});

test("signing out ends the session and opens sign-in", async () => {
  const api = new FakeApi().on("POST", "/auth/logout", new Response(null, { status: 204 }));
  const session = await signedIn(api);
  await renderWith(session, <EmployeeNav />);
  await fireEvent.press(screen.getByTestId("sign-out-btn"));
  await waitFor(() => expect(router.replace).toHaveBeenCalledWith("/sign-in"));
  expect(session.signedIn).toBe(false);
});
