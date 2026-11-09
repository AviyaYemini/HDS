/**
 * The roster's assign sheet says once where someone is already placed (owner, 2026-11-09; 008
 * T042f, FR-034): the project and the time when the reader may see that shift, the time alone
 * otherwise, and never again as the ranking's "already on another shift".
 */
import { render, screen } from "@testing-library/react-native";

import { AssignSheet } from "../../../src/screens/roster/AssignSheet";
import type { BusyOn, Cover, Shift } from "../../../src/screens/roster/model";

const ANN = "44444444-4444-4444-8444-444444444444";
const BEN = "55555555-5555-4555-8555-555555555555";
const ELSEWHERE = "66666666-6666-4666-8666-666666666666";

const shift = {
  id: "s1",
  project_id: "p1",
  shift_type_id: "t1",
  starts_at: "2027-06-06T04:00:00Z",
  ends_at: "2027-06-06T12:00:00Z",
  timezone: "Asia/Jerusalem",
  headcount: 2,
  status: "draft",
  etag: '"1"',
  assignments: [],
} as unknown as Shift;

const busy = [
  { user_id: ANN, project_id: ELSEWHERE, shift_id: "s9", assignment_id: "a9", starts_at: "2027-06-06T04:00:00Z", ends_at: "2027-06-06T12:00:00Z", movable: true },
  { user_id: BEN, project_id: null, shift_id: null, assignment_id: null, starts_at: "2027-06-06T05:00:00Z", ends_at: "2027-06-06T09:00:00Z", movable: false },
] as BusyOn[];

const ranked = {
  options: [
    { user_id: ANN, free: false, close: false, code: "schedule.overlap" },
    { user_id: BEN, free: false, close: false, code: "schedule.overlap" },
  ],
} as unknown as Cover;

test("a busy person is said once, with the project when it may be seen, else the time", async () => {
  await render(
    <AssignSheet
      shift={shift}
      people={[
        { id: ANN, full_name: "Ann" },
        { id: BEN, full_name: "Ben" },
      ]}
      busy={busy}
      ranked={ranked}
      projectName={(id) => (id === ELSEWHERE ? "Night Watch" : null)}
      replacing={null}
      note=""
      alert="none"
      onNote={() => undefined}
      onAlert={() => undefined}
      onClose={() => undefined}
      onChoose={() => undefined}
    />,
  );

  const lines = screen.getAllByTestId("assign-busy").map((n) => String(n.props.children));
  expect(lines[0]).toMatch(/^On Night Watch then, /);
  expect(lines[1]).toMatch(/^On another shift then, /);
  expect(screen.queryByText("Already on another shift")).toBeNull();
});
