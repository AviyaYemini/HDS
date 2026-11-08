/** Sites: built by a later group of 007; a guarded placeholder until then (FR-007). */
import { RequireCapability } from "../../../src/capabilities";
import { PlaceholderScreen } from "../../../src/screens/misc/HubScreen";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";

export default function Route() {
  return (
    <RequireCapability need="sites.manage" denied={<NoAccessScreen />}>
      <PlaceholderScreen titleKey="nav.sites" bodyKey="placeholder.admin" eyebrowKey="nav.group.admin" testID="screen-admin-sites" />
    </RequireCapability>
  );
}
