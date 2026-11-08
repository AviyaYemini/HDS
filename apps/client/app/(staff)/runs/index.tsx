/** Schedule runs: the list and the generate form (specs/007-ux-redesign T079, FR-042, FR-007). */
import { RequireCapability } from "../../../src/capabilities";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";
import { RunsScreen } from "../../../src/screens/runs/RunsScreen";

export default function Route() {
  return (
    <RequireCapability need="schedule.generate" denied={<NoAccessScreen />}>
      <RunsScreen />
    </RequireCapability>
  );
}
