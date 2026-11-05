// Sign-in (001 US1, 003 US4): success goes to My availability; refusals are shown translated.
import { fireEvent, screen, waitFor } from "@testing-library/react-native";
import * as Router from "expo-router";

import { Session } from "../../src/api/session";
import { memoryStore } from "../../src/api/tokens";
import { SignInScreen } from "../../src/screens/SignInScreen";
import { BASE, FakeApi, ME, TOKENS, json, problem, renderWith } from "./fakeApi";

const router = (Router as unknown as { __router: { replace: jest.Mock } }).__router;

async function fill(): Promise<void> {
  await fireEvent.changeText(screen.getByTestId("email"), " employee@hds.test ");
  await fireEvent.changeText(screen.getByTestId("password"), "Dev1234!");
  await fireEvent.press(screen.getByTestId("sign-in-btn"));
}

beforeEach(() => jest.clearAllMocks());

test("a good sign-in opens My availability", async () => {
  const api = new FakeApi().on("POST", "/auth/login", json(200, TOKENS)).on("GET", "/me", json(200, ME));
  await renderWith(new Session(BASE, memoryStore(), api.fetch), <SignInScreen />);
  await fill();
  await waitFor(() => expect(router.replace).toHaveBeenCalledWith("/availability"));
  expect(api.calls("POST", "/auth/login")[0]!.body).toEqual({
    email: "employee@hds.test",
    password: "Dev1234!",
  });
});

test.each([
  ["auth.invalid_credentials", 401, "Invalid email or password."],
  ["auth.account_inactive", 403, "This account is inactive."],
  ["auth.rate_limited", 429, "Too many failed sign-ins from this network; try again later."],
  ["something.new", 400, "Something went wrong. Please try again."],
])("a refusal with %s is shown translated", async (code, status, text) => {
  const api = new FakeApi().on("POST", "/auth/login", problem(status, code));
  await renderWith(new Session(BASE, memoryStore(), api.fetch), <SignInScreen />);
  await fill();
  expect(await screen.findByTestId("error-message")).toHaveTextContent(text);
  expect(router.replace).not.toHaveBeenCalled();
});

test("a locked account explains when to try again", async () => {
  const api = new FakeApi().on("POST", "/auth/login", problem(423, "auth.account_locked"));
  await renderWith(new Session(BASE, memoryStore(), api.fetch), <SignInScreen />);
  await fill();
  const message = await screen.findByTestId("error-message");
  expect(message).toHaveTextContent(/Account locked/);
  expect(message).toHaveTextContent(/try again later/);
});
