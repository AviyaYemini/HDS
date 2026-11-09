/** Special days: the admin table and record panel (specs/007-ux-redesign T083 to T085, FR-007). */
import { RequireCapability } from "../../../src/capabilities";
import { SpecialDaysScreen } from "../../../src/screens/admin";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";

export default function Route() {
  return (
    <RequireCapability need="special_days.edit" denied={<NoAccessScreen />}>
      <SpecialDaysScreen />
    </RequireCapability>
  );
}
