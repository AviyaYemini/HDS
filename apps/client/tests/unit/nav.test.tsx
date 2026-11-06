// The employee bar (003 US4, US7; 001 US1 sign-out): moving between the screens, and signing out.
import { fireEvent, screen, waitFor } from "@testing-library/react-native";
import * as Router from "expo-router";

import { EmployeeNav } from "../../src/components/EmployeeNav";
import { FakeApi, json, renderWith, signedIn } from "./fakeApi";

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

test("everyone who works shifts reaches My pay; only Managers and Admins see the reports", async () => {
  const employee = new FakeApi().on("GET", "/me", json(200, { id: "u1", role: "employee" }));
  await renderWith(await signedIn(employee), <EmployeeNav />);
  await fireEvent.press(await screen.findByTestId("nav-pay"));
  expect(router.push).toHaveBeenLastCalledWith("/pay");
  expect(screen.queryByTestId("nav-work-log")).toBeNull();
  expect(screen.queryByTestId("nav-hours")).toBeNull();
});

test("a Manager moves to the work-log and the hours report", async () => {
  const manager = new FakeApi().on("GET", "/me", json(200, { id: "u2", role: "manager" }));
  await renderWith(await signedIn(manager), <EmployeeNav />);
  await fireEvent.press(await screen.findByTestId("nav-work-log"));
  expect(router.push).toHaveBeenLastCalledWith("/reports/work-log");
  await fireEvent.press(screen.getByTestId("nav-hours"));
  expect(router.push).toHaveBeenLastCalledWith("/reports/hours");
});
