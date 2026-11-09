/** Who has submitted availability for a window (specs/007-ux-redesign T074, FR-040, FR-007). */
import { RequireCapability } from "../../../src/capabilities";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";
import { SubmissionsScreen } from "../../../src/screens/roster";

export default function Route() {
  return (
    <RequireCapability need="availability.read_others" denied={<NoAccessScreen />}>
      <SubmissionsScreen />
    </RequireCapability>
  );
}
