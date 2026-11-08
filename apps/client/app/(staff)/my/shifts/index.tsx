/** My shifts; today's screen until group C rebuilds it. */
import { RequireCapability } from "../../../../src/capabilities";
import { MyShiftsScreen } from "../../../../src/screens/MyShiftsScreen";
import { NoAccessScreen } from "../../../../src/screens/no-access/NoAccessScreen";

export default function Route() {
  return (
    <RequireCapability need="my_work.read" denied={<NoAccessScreen />}>
      <MyShiftsScreen />
    </RequireCapability>
  );
}
