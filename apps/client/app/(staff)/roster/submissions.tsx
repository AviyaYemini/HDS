/** Submissions: built by a later group of 007; a guarded placeholder until then (FR-007). */
import { RequireCapability } from "../../../src/capabilities";
import { PlaceholderScreen } from "../../../src/screens/misc/HubScreen";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";

export default function Route() {
  return (
    <RequireCapability need="availability.read_others" denied={<NoAccessScreen />}>
      <PlaceholderScreen titleKey="nav.submissions" bodyKey="placeholder.submissions" eyebrowKey="nav.group.manage" testID="screen-submissions" />
    </RequireCapability>
  );
}
