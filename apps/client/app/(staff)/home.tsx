/** Home per role (specs/007-ux-redesign FR-006). */
import { RequireCapability } from "../../src/capabilities";
import { NoAccessScreen } from "../../src/screens/no-access/NoAccessScreen";
import { HomeScreen } from "../../src/screens/home/HomeScreen";

export default function Route() {
  return (
    <RequireCapability need="my_work.read" denied={<NoAccessScreen />}>
      <HomeScreen />
    </RequireCapability>
  );
}
