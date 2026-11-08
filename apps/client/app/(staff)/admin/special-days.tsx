/** Special days: built by a later group of 007; a guarded placeholder until then (FR-007). */
import { RequireCapability } from "../../../src/capabilities";
import { PlaceholderScreen } from "../../../src/screens/misc/HubScreen";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";

export default function Route() {
  return (
    <RequireCapability need="special_days.edit" denied={<NoAccessScreen />}>
      <PlaceholderScreen titleKey="nav.special_days" bodyKey="placeholder.admin" eyebrowKey="nav.group.admin" testID="screen-admin-special-days" />
    </RequireCapability>
  );
}
