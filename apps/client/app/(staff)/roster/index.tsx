/** Roster: built by a later group of 007; a guarded placeholder until then (FR-007). */
import { RequireCapability } from "../../../src/capabilities";
import { PlaceholderScreen } from "../../../src/screens/misc/HubScreen";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";

export default function Route() {
  return (
    <RequireCapability need="roster.read" denied={<NoAccessScreen />}>
      <PlaceholderScreen titleKey="nav.roster" bodyKey="placeholder.roster" eyebrowKey="nav.group.manage" testID="screen-roster" />
    </RequireCapability>
  );
}
