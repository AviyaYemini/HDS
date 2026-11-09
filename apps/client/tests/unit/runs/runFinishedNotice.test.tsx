/**
 * The in-app notice when a run ends (specs/008-auto-scheduler-review FR-029): a run of the person's
 * seen running and then ended is told once, with a way to open it; not on that run's own page, and
 * never for someone else's run.
 */
import { act, fireEvent, render, screen } from "@testing-library/react-native";
import * as Router from "expo-router";

import { SessionProvider } from "../../../src/api/SessionContext";
import { ToastHost, clearToasts } from "../../../src/design/components";
import { RunFinishedNotice } from "../../../src/screens/runs/RunFinishedNotice";
import { FakeApi, ME, json, signedIn } from "../fakeApi";

const RUN = "55555555-5555-4555-8555-555555555555";
const OTHER = "66666666-6666-4666-8666-666666666666";
const JOBS = "/api/v1/schedule-jobs";
const router = (Router as unknown as { __router: { push: jest.Mock } }).__router;
const setPathname = (Router as unknown as { __setPathname: (p: string) => void }).__setPathname;

const job = (id: string, status: string, by = ME.id) => ({ id, status, requested_by: by });
const page = (...items: ReturnType<typeof job>[]) => json(200, { items, next_cursor: null });

async function look(ms: number) {
  await act(async () => {
    await jest.advanceTimersByTimeAsync(ms);
    for (let i = 0; i < 50; i += 1) await Promise.resolve();
  });
}

beforeEach(() => {
  clearToasts();
  router.push.mockClear();
  setPathname("/roster");
});

afterEach(() => {
  jest.useRealTimers();
});

async function mount(api: FakeApi) {
  const session = await signedIn(api);
  jest.useFakeTimers();
  await render(
    <SessionProvider session={session}>
      <ToastHost />
      <RunFinishedNotice />
    </SessionProvider>,
  );
  await look(0);
}

test("a run seen running and then finished is told once, and opens", async () => {
  const api = new FakeApi().on("GET", JOBS, page(job(RUN, "running"), job(OTHER, "running", "someone")), page(job(RUN, "succeeded"), job(OTHER, "failed", "someone")));
  await mount(api);
  expect(screen.queryByTestId("toast-run-finished")).toBeNull();

  await look(5_000);
  expect(screen.getAllByTestId("toast-run-finished")).toHaveLength(1);
  await act(async () => {
    fireEvent.press(screen.getByRole("button", { name: "Open" }));
  });
  expect(router.push).toHaveBeenCalledWith(`/runs/${RUN}`);

  // The toast leaves on its own; the next look tells nothing new.
  await look(30_000);
  expect(screen.queryByTestId("toast-run-finished")).toBeNull();
});

test("a run that started and ended between two looks is told too, but not on its own page", async () => {
  const api = new FakeApi().on("GET", JOBS, page(), page(job(RUN, "failed")));
  setPathname(`/runs/${RUN}`);
  await mount(api);
  await look(30_000);
  expect(screen.queryByTestId("toast-run-finished")).toBeNull();
});

test("a failed look waits for the next one", async () => {
  const api = new FakeApi().on("GET", JOBS, json(500, {}), page(job(RUN, "running")), page(job(RUN, "cancelled")));
  await mount(api);
  await look(30_000);
  await look(5_000);
  expect(screen.getByTestId("toast-run-finished")).toBeTruthy();
});

test("a run pushed off the newest page by others is read by itself, and its end told", async () => {
  const others = Array.from({ length: 20 }, (_, i) => job(`7777777${i.toString(16)}-7777-4777-8777-777777777777`, "running", "someone"));
  const api = new FakeApi()
    .on("GET", JOBS, page(job(RUN, "running")), page(...others))
    .on("GET", `${JOBS}/${RUN}`, json(200, job(RUN, "succeeded")));
  await mount(api);
  await look(5_000);
  expect(screen.getByTestId("toast-run-finished")).toBeTruthy();
});
