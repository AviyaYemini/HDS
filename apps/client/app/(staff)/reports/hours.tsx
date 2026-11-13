/** Hours and costs in the deck's style (CEO demo, slide 9). */
import { RequireCapability } from "../../../src/capabilities";
import { HoursCostsScreen } from "../../../src/screens/reports/HoursCostsScreen";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";

export default function Route() {
  return (
    <RequireCapability need="reports.read" denied={<NoAccessScreen />}>
      <HoursCostsScreen />
    </RequireCapability>
  );
}
