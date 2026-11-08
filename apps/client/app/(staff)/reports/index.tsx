/** The Reports hub, a phone tab for managers and admins. */
import { RequireCapability } from "../../../src/capabilities";
import { HubScreen } from "../../../src/screens/misc/HubScreen";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";

export default function Route() {
  return (
    <RequireCapability need="reports.read" denied={<NoAccessScreen />}>
      <HubScreen titleKey="nav.reports" groups={["manage"]} testID="screen-reports" />
    </RequireCapability>
  );
}
