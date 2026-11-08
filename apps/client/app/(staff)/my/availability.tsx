/** My availability; today's screen until group C rebuilds it (003 FR-1xx; 007 contracts/ui-routes.md). */
import { RequireCapability } from "../../../src/capabilities";
import { AvailabilityScreen } from "../../../src/screens/AvailabilityScreen";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";

export default function Route() {
  return (
    <RequireCapability need="my_work.availability" denied={<NoAccessScreen />}>
      <AvailabilityScreen />
    </RequireCapability>
  );
}
