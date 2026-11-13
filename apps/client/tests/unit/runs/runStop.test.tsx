/**
 * The run page while it works (the owner, 2026-11-13): the live count as figure 01 with a bar, the
 * scope on one line, and the stop button, so the jump to the finished page is calm.
 */
import { fireEvent, screen } from "@testing-library/react-native";

import { setReducedMotionForTests } from "../../../src/design/motion";
import { setupI18n } from "../../../src/i18n";
import { type Job } from "../../../src/screens/runs/model";
import { RunScope } from "../../../src/screens/runs/RunScope";
import { RunStop } from "../../../src/screens/runs/RunStop";
import { FakeApi, json, renderWith, signedIn } from "../fakeApi";

beforeEach(async () => {
  await setupI18n("en");
  setReducedMotionForTests(true);
});

const job = { id: "j1", status: "running", from: "2026-11-15", to: "2026-11-21", project_ids: ["p1", "p2", "p3"], progress: { filled: 12, needed: 40 } } as unknown as Job;

test("the best so far shows as figure 01 with a bar, above the stop button", async () => {
  await renderWith(await signedIn(new FakeApi()), <RunStop job={job} onStopped={() => undefined} />);
  expect(await screen.findByTestId("run-best")).toHaveTextContent("12/40");
  expect(screen.getByTestId("run-best-so-far")).toHaveTextContent(/01/);
  expect(screen.getByTestId("run-best-bar")).toBeTruthy();
  expect(screen.getByTestId("action-stop-run")).toBeTruthy();
});

test("without a result yet there is no figure, only the button", async () => {
  await renderWith(await signedIn(new FakeApi()), <RunStop job={{ ...job, progress: null } as unknown as Job} onStopped={() => undefined} />);
  expect(await screen.findByTestId("action-stop-run")).toBeTruthy();
  expect(screen.queryByTestId("run-best")).toBeNull();
  expect(screen.queryByTestId("run-best-bar")).toBeNull();
});

test("the scope is one line with the names behind a toggle", async () => {
  const api = new FakeApi().on("GET", "/api/v1/projects", json(200, { items: [
    { id: "p1", name: "Harbour gate", type: "security" },
    { id: "p2", name: "City museum", type: "security" },
    { id: "p3", name: "Night watch", type: "security" },
    { id: "p4", name: "Old port", type: "security" },
  ] }));
  const names = { projects: new Map([["p1", "Harbour gate"], ["p2", "City museum"], ["p3", "Night watch"]]), types: new Map(), people: new Map() };
  await renderWith(await signedIn(api), <RunScope job={job} names={names as never} />);
  const scope = await screen.findByTestId("run-projects-covered");
  expect(scope).toHaveTextContent(/3/);
  expect(screen.queryByText(/Harbour gate, City museum, Night watch/)).toBeNull();
  await fireEvent.press(screen.getByTestId("run-projects-covered-toggle"));
  expect(await screen.findByText(/Harbour gate/)).toBeTruthy();
});
