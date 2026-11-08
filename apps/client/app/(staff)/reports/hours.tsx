/** The hours report; today's screen until group G rebuilds it. */
import { RequireCapability } from "../../../src/capabilities";
import { HoursReportScreen } from "../../../src/screens/reports/ReportScreens";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";

export default function Route() {
  return (
    <RequireCapability need="reports.read" denied={<NoAccessScreen />}>
      <HoursReportScreen />
    </RequireCapability>
  );
}
