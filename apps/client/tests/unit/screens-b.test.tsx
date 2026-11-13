/**
 * Group B's screens (specs/007-ux-redesign FR-006, FR-007, FR-009, FR-012, FR-077, task T045):
 * the no-access page reads the account again and offers a way home, the 404 page has one too,
 * the home draws the next shift from the API, the Account screen saves the phone and ends only
 * other sessions (none at all when the server does not mark the current one), and the contact
 * card shows what the API holds.
 */
import { fireEvent, screen, waitFor } from "@testing-library/react-native";

import { Session, type Me } from "../../src/api/session";
import { memoryStore } from "../../src/api/tokens";
import { SignInRedirect, queryOnly } from "../../src/auth/SignInRedirect";
import { clearSessionEnded, isSessionEnded, markSessionEnded, noteRestore } from "../../src/auth/sessionEnded";
import { SetPasswordScreen } from "../../src/screens/misc/SetPasswordScreen";
import { SignInScreen } from "../../src/screens/SignInScreen";
import { AccountScreen } from "../../src/screens/account/AccountScreen";
import { ContactCard } from "../../src/screens/client/ClientScreens";
import { HomeScreen } from "../../src/screens/home/HomeScreen";
import { NotFoundScreen } from "../../src/screens/misc/NotFoundScreen";
import { NoAccessScreen } from "../../src/screens/no-access/NoAccessScreen";
import { BASE, FakeApi, ME, TOKENS, json, problem, renderWith, signedIn } from "./fakeApi";
import * as Router from "expo-router";

const router = (Router as unknown as { __router: { replace: jest.Mock; push: jest.Mock } }).__router;

const EMPLOYEE_ME: Me = { ...(ME as Me), capabilities: ["my_work.read", "my_work.availability", "account.self"], managed_project_ids: [] };

beforeEach(() => jest.clearAllMocks());

describe("NoAccessScreen (FR-007)", () => {
  it("reads the account again and goes home", async () => {
    const api = new FakeApi().on("GET", "/me", json(200, EMPLOYEE_ME));
    const session = await signedIn(api);
    const reads = api.calls("GET", "/me").length;
    await renderWith(session, <NoAccessScreen />);
    expect(screen.getByTestId("screen-no-access")).toBeTruthy();
    expect(api.calls("GET", "/me").length).toBeGreaterThan(reads);
    await fireEvent.press(screen.getByTestId("action-go-home"));
    expect(router.replace).toHaveBeenCalledWith("/home");
  });
});

describe("NotFoundScreen (FR-077)", () => {
  it("offers the role's home", async () => {
    const session = await signedIn(new FakeApi().on("GET", "/me", json(200, EMPLOYEE_ME)));
    await renderWith(session, <NotFoundScreen />);
    expect(screen.getByTestId("screen-not-found")).toBeTruthy();
    await fireEvent.press(screen.getByTestId("action-go-home"));
    expect(router.replace).toHaveBeenCalledWith("/home");
  });
});

describe("HomeScreen (FR-006)", () => {
  it("shows the employee's next shift and the open window's deadline", async () => {
    const api = new FakeApi()
      .on("GET", "/me", json(200, EMPLOYEE_ME))
      .on(
        "GET",
        "/me/shifts",
        json(200, {
          items: [
            {
              id: "s1",
              starts_at: "2026-11-09T20:00:00Z",
              ends_at: "2026-11-10T04:00:00Z",
              timezone: "Asia/Jerusalem",
              project: { id: "p", name: "Night Watch" },
              site: { id: "x", name: "Gate 3" },
              shift_type: "Night",
            },
          ],
        }),
      )
      .on("GET", "/api/v1/availability-windows", json(200, { items: [{ id: "w", starts_on: "2026-11-15", ends_on: "2026-11-21", deadline: "2026-11-12T10:00:00Z", is_open: true }] }));
    await renderWith(await signedIn(api), <HomeScreen />);
    expect(await screen.findByTestId("home-next-shift")).toBeTruthy();
    expect(screen.getByText("\u2068Night Watch\u2069 · \u2068Night\u2069")).toBeTruthy();
    expect(screen.getByText(/22:00–06:00 \+1/)).toBeTruthy();
    expect(await screen.findByTestId("home-deadline")).toHaveTextContent(/12\/11\/2026/);
    // "Open availability" goes to the availability screen (R27).
    await fireEvent.press(screen.getByTestId("home-deadline-open"));
    expect(router.push).toHaveBeenCalledWith("/my/availability");
  });

  it("an Admin's home counts drafts across the coming weeks and hides set-up once there are projects and staff", async () => {
    const admin: Me = { ...EMPLOYEE_ME, role: "admin", capabilities: [...EMPLOYEE_ME.capabilities, "roster.read", "special_days.edit", "staff.manage"] };
    const draft = (id: string, starts: string) => ({ id, project_id: "p", shift_type_id: "t", starts_at: starts, ends_at: starts, timezone: "Asia/Jerusalem", headcount: 1, status: "draft", etag: "e", pay_rate: null, published_at: null, assignments: [] });
    const api = new FakeApi()
      .on("GET", "/me", json(200, admin))
      .on("GET", "/me/shifts", json(200, { items: [] }))
      .on("GET", "/api/v1/availability-windows", json(200, { items: [] }))
      .on("GET", "/api/v1/projects", json(200, { items: [{ id: "p", name: "Harbour gate" }] }))
      .on("GET", "/api/v1/employees", json(200, { items: [{ id: "u9", role: "employee", full_name: "Dana Levi" }] }))
      .on("GET", "/api/v1/shifts", (seen) =>
        json(200, new URLSearchParams(seen.query).get("status") === "draft" ? { items: [draft("d2", "2026-12-15T05:00:00Z"), draft("d1", "2026-12-08T05:00:00Z")], next_cursor: null } : { items: [], next_cursor: null }),
      );
    await renderWith(await signedIn(api), <HomeScreen />);
    // Two drafts in two weeks; the card opens the first of those weeks.
    await waitFor(() => expect(screen.getByTestId("home-card-unpublished")).toHaveTextContent(/2$/));
    const asked = new URLSearchParams(api.calls("GET", "/api/v1/shifts").find((c) => c.query.includes("status=draft"))!.query);
    expect(Number(new Date(asked.get("to")!)) - Number(new Date(asked.get("from")!))).toBe(27 * 86_400_000);
    await waitFor(() => expect(screen.queryByTestId("home-setup")).toBeNull());
  });

  it("shows placeholders, never numbers, on the manager's cards", async () => {
    const manager: Me = { ...EMPLOYEE_ME, role: "manager", capabilities: [...EMPLOYEE_ME.capabilities, "roster.read", "schedule.generate", "availability.read_others"] };
    const api = new FakeApi().on("GET", "/me", json(200, manager)).on("GET", "/me/shifts", json(200, { items: [] })).on("GET", "/api/v1/availability-windows", json(200, { items: [] }));
    await renderWith(await signedIn(api), <HomeScreen />);
    for (const id of ["home-card-coverage", "home-card-unfilled", "home-card-not-submitted"]) expect(screen.getByTestId(id)).toBeTruthy();
    expect(screen.queryByTestId("home-setup")).toBeNull();
    expect(await screen.findByTestId("home-next-shift-none")).toBeTruthy();
    await fireEvent.press(screen.getByTestId("home-open-availability"));
    expect(router.push).toHaveBeenCalledWith("/my/availability");
  });
});

describe("AccountScreen (FR-009, FR-010)", () => {
  const sessions = [
    { id: "a", client_kind: "web", expires_at: "2026-11-14T00:00:00Z", last_refreshed_at: "2026-11-08T09:00:00Z", current: true },
    { id: "b", client_kind: "native", expires_at: "2026-12-08T00:00:00Z", last_refreshed_at: "2026-11-07T09:00:00Z", current: false },
  ];

  it("saves the phone and asks the server to end the other sessions", async () => {
    const api = new FakeApi()
      .on("GET", "/me", json(200, EMPLOYEE_ME))
      .on("PATCH", "/me", json(200, { ...EMPLOYEE_ME, phone: "+972501234567" }))
      .on("GET", "/me/sessions", json(200, sessions))
      .on("POST", "/auth/logout-others", new Response(null, { status: 204 }));
    await renderWith(await signedIn(api), <AccountScreen />);
    expect(screen.getByTestId("screen-account")).toBeTruthy();
    await fireEvent.changeText(screen.getByTestId("profile-phone"), "+972501234567");
    await fireEvent.press(screen.getByTestId("action-save-profile"));
    expect(api.calls("PATCH", "/me")[0]!.body).toMatchObject({ phone: "+972501234567" });
    await screen.findByTestId("session-a");
    expect(screen.getByText("This device")).toBeTruthy();
    expect(screen.queryByTestId("action-end-session-a")).toBeNull();
    await fireEvent.press(screen.getByTestId("action-sign-out-others"));
    await screen.findByTestId("session-a");
    expect(api.calls("POST", "/auth/logout-others").length).toBe(1);
    expect(api.seen.filter((seen) => seen.method === "DELETE").length).toBe(0);
  });

  it("shows the server's reason when it cannot tell which device this is", async () => {
    const unmarked = sessions.map((item) => ({ ...item, current: false }));
    const api = new FakeApi()
      .on("GET", "/me", json(200, EMPLOYEE_ME))
      .on("GET", "/me/sessions", json(200, unmarked))
      .on("POST", "/auth/logout-others", problem(409, "session.current_unknown"));
    await renderWith(await signedIn(api), <AccountScreen />);
    await screen.findByTestId("session-a");
    await fireEvent.press(screen.getByTestId("action-sign-out-others"));
    expect(await screen.findByTestId("sign-out-others-error")).toHaveTextContent(/Sign in again/);
    expect(api.seen.filter((seen) => seen.method === "DELETE").length).toBe(0);
  });

  it("switches the language radios", async () => {
    const api = new FakeApi().on("GET", "/me", json(200, EMPLOYEE_ME)).on("GET", "/me/sessions", json(200, []));
    await renderWith(await signedIn(api), <AccountScreen />);
    expect(screen.getByTestId("language-en").props.accessibilityState.checked).toBe(true);
    expect(screen.getByTestId("language-he").props.accessibilityState.checked).toBe(false);
  });
});

describe("ContactCard (FR-012)", () => {
  it("shows phone, e-mail and opening hours", async () => {
    const api = new FakeApi()
      .on("GET", "/me", json(200, EMPLOYEE_ME))
      .on("GET", "/api/v1/organisation/contact", json(200, { phone: "03-5550100", email: "office@hds.test", opening_hours: [{ weekday: 0, opens: "08:00", closes: "17:00" }] }));
    await renderWith(await signedIn(api), <ContactCard testID="card" />);
    expect(await screen.findByTestId("contact-phone")).toHaveTextContent(/03-5550100/);
    expect(screen.getByTestId("contact-email")).toHaveTextContent(/office@hds\.test/);
    expect(screen.getByTestId("contact-hours-0")).toHaveTextContent(/Sunday/);
    expect(screen.getByTestId("contact-hours-0")).toHaveTextContent(/08:00–17:00/);
  });
});

describe("SignInRedirect without previews (FR-068)", () => {
  // Jest builds without EXPO_PUBLIC_PREVIEWS, so the previews are off here.
  const setPathname = (Router as unknown as { __setPathname: (pathname: string) => void }).__setPathname;
  afterEach(() => setPathname("/"));

  it("shows the 404 page for a preview address instead of sending to sign-in", async () => {
    setPathname("/client/billing");
    await renderWith(new Session(BASE, memoryStore(), new FakeApi().fetch), <SignInRedirect />);
    expect(screen.getByTestId("screen-not-found")).toBeTruthy();
    expect(screen.queryByTestId("redirect")).toBeNull();
  });

  it("still sends an ordinary address to sign-in", async () => {
    setPathname("/my/shifts");
    await renderWith(new Session(BASE, memoryStore(), new FakeApi().fetch), <SignInRedirect />);
    expect(screen.queryByTestId("screen-not-found")).toBeNull();
    expect(screen.getByTestId("redirect")).toHaveTextContent("/sign-in?next=%2Fmy%2Fshifts");
  });
});

describe("the session-ended notice after a restore (R25)", () => {
  afterEach(() => clearSessionEnded());

  it.each([
    [true, false, true, true, "kept tokens the server refused"],
    [true, false, false, false, "kept tokens with the server out of reach"],
    [false, false, false, false, "no kept tokens"],
    [true, true, false, false, "a restored session"],
  ])("hadTokens=%s restored=%s endedUnexpectedly=%s → notice %s (%s)", (hadTokens, restored, endedUnexpectedly, shown) => {
    noteRestore(hadTokens, restored, endedUnexpectedly);
    expect(isSessionEnded()).toBe(shown);
  });

  it("clears an older notice when the session is restored", () => {
    markSessionEnded();
    noteRestore(true, true, false);
    expect(isSessionEnded()).toBe(false);
  });
});

describe("SetPasswordScreen (FR-011, R26)", () => {
  const setSearchParams = (Router as unknown as { __setSearchParams: (params: Record<string, string>) => void }).__setSearchParams;
  afterEach(() => setSearchParams({}));

  it("needs the confirmation to match before the password can be set", async () => {
    setSearchParams({ token: "inv-1" });
    const api = new FakeApi().on("POST", "/auth/invite/accept", new Response(null, { status: 204 }));
    await renderWith(new Session(BASE, memoryStore(), api.fetch), <SetPasswordScreen />);
    const button = () => screen.getByTestId("action-set-password");
    expect(button()).toBeDisabled();
    await fireEvent.changeText(screen.getByTestId("new-password"), "Abcdefgh1");
    expect(button()).toBeDisabled();
    await fireEvent(screen.getByTestId("new-password-again"), "submitEditing");
    expect(api.calls("POST", "/auth/invite/accept")).toHaveLength(0);
    await fireEvent.changeText(screen.getByTestId("new-password-again"), "Abcdefgh2");
    expect(screen.getByText("The two passwords differ.")).toBeTruthy();
    expect(button()).toBeDisabled();
    await fireEvent.changeText(screen.getByTestId("new-password-again"), "Abcdefgh1");
    expect(button()).not.toBeDisabled();
    await fireEvent.press(button());
    await waitFor(() => expect(api.calls("POST", "/auth/invite/accept")).toHaveLength(1));
    expect(api.calls("POST", "/auth/invite/accept")[0]?.body).toEqual({ token: "inv-1", password: "Abcdefgh1" });
    expect(router.replace).toHaveBeenCalledWith("/sign-in");
  });
});

describe("SignInScreen submits once (R29)", () => {
  it("ignores a second Enter while the first sign-in is in flight", async () => {
    // The server answers only when the test lets it, so the second Enter lands while the first is in flight.
    let answer: (() => void) | null = null;
    const api = new FakeApi()
      .on("POST", "/auth/login", () => new Promise<Response>((resolve) => (answer = () => resolve(json(200, TOKENS)))))
      .on("GET", "/me", json(200, EMPLOYEE_ME));
    await renderWith(new Session(BASE, memoryStore(), api.fetch), <SignInScreen />);
    await fireEvent.changeText(screen.getByTestId("email"), "employee@hds.test");
    await fireEvent.changeText(screen.getByTestId("password"), "Dev1234!");
    await fireEvent(screen.getByTestId("password"), "submitEditing");
    await fireEvent(screen.getByTestId("password"), "submitEditing");
    await fireEvent.press(screen.getByTestId("sign-in-btn"));
    expect(api.calls("POST", "/auth/login")).toHaveLength(1);
    await waitFor(() => expect(answer).not.toBeNull());
    answer!();
    await waitFor(() => expect(router.replace).toHaveBeenCalledWith("/home"));
    expect(api.calls("POST", "/auth/login")).toHaveLength(1);
  });
});

describe("ending a renewed session (PR 2 review)", () => {
  it("on a 404 reloads the list and says the session was just renewed", async () => {
    const list = (id: string) => json(200, [{ id, device: "Phone", created_at: "2026-11-01T00:00:00Z", last_refreshed_at: "2026-11-08T10:00:00Z", current: false }]);
    const api = new FakeApi()
      .on("GET", "/me", json(200, EMPLOYEE_ME))
      .on("GET", "/me/sessions", list("old"), list("new"))
      .on("DELETE", "/me/sessions/old", problem(404, "resource.not_found"));
    await renderWith(await signedIn(api), <AccountScreen />);
    await fireEvent.press(await screen.findByTestId("action-end-session-old"));
    expect(await screen.findByTestId("session-renewed")).toHaveTextContent(/just renewed/);
    expect(api.calls("GET", "/me/sessions")).toHaveLength(2);
    expect(await screen.findByTestId("action-end-session-new")).toBeTruthy();
    expect(screen.queryByTestId("action-end-session-old")).toBeNull();
  });
});

describe("SignInRedirect keeps only the query (PR 2 review)", () => {
  const setPathname = (Router as unknown as { __setPathname: (pathname: string) => void }).__setPathname;
  const setSearchParams = (Router as unknown as { __setSearchParams: (params: Record<string, string>) => void }).__setSearchParams;
  const setSegments = (Router as unknown as { __setSegments: (segments: string[]) => void }).__setSegments;
  afterEach(() => {
    setPathname("/");
    setSearchParams({});
    setSegments([]);
  });

  it("leaves the route's dynamic segment and lang out of next", async () => {
    setPathname("/my/shifts/abc");
    setSegments(["(staff)", "my", "shifts", "[id]"]);
    setSearchParams({ id: "abc", lang: "he", view: "past" });
    await renderWith(new Session(BASE, memoryStore(), new FakeApi().fetch), <SignInRedirect />);
    expect(screen.getByTestId("redirect")).toHaveTextContent(`/sign-in?next=${encodeURIComponent("/my/shifts/abc?view=past")}&lang=he`);
  });

  it("drops a catch-all segment too", () => {
    expect(queryOnly({ rest: ["a", "b"], q: "1" }, ["(staff)", "docs", "[...rest]"])).toEqual({ q: "1" });
  });
});
