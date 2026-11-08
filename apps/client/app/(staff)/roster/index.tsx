/** Roster: the week of a project (specs/007-ux-redesign T070 to T074, FR-007). */
import { RequireCapability } from "../../../src/capabilities";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";
import { RosterScreen } from "../../../src/screens/roster/RosterScreen";

export default function Route() {
  return (
    <RequireCapability need="roster.read" denied={<NoAccessScreen />}>
      <RosterScreen />
    </RequireCapability>
  );
}
