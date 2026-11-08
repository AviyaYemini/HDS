/** One schedule run: its progress and proposal (specs/007-ux-redesign T079, FR-042, FR-074, FR-007). */
import { RequireCapability } from "../../../src/capabilities";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";
import { RunScreen } from "../../../src/screens/runs/RunScreen";

export default function Route() {
  return (
    <RequireCapability need="schedule.generate" denied={<NoAccessScreen />}>
      <RunScreen />
    </RequireCapability>
  );
}
