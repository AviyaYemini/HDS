/** My pay; today's screen until group C rebuilds it. */
import { RequireCapability } from "../../../src/capabilities";
import { MyPayScreen } from "../../../src/screens/MyPayScreen";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";

export default function Route() {
  return (
    <RequireCapability need="my_work.read" denied={<NoAccessScreen />}>
      <MyPayScreen />
    </RequireCapability>
  );
}
