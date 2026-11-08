/** The work log; today's screen until group G rebuilds it. */
import { RequireCapability } from "../../../src/capabilities";
import { WorkLogScreen } from "../../../src/screens/reports/ReportScreens";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";

export default function Route() {
  return (
    <RequireCapability need="reports.read" denied={<NoAccessScreen />}>
      <WorkLogScreen />
    </RequireCapability>
  );
}
