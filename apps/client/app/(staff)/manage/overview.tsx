/** The snapshot dashboard (CEO demo, slide 14): where an Admin or a Manager lands. */
import { RequireCapability } from "../../../src/capabilities";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";
import { OverviewScreen } from "../../../src/screens/overview/OverviewScreen";

export default function Route() {
  return (
    <RequireCapability need="reports.read" denied={<NoAccessScreen />}>
      <OverviewScreen />
    </RequireCapability>
  );
}
