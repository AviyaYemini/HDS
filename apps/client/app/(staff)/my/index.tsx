/** The My work hub, a phone tab for managers and admins. */
import { RequireCapability } from "../../../src/capabilities";
import { HubScreen } from "../../../src/screens/misc/HubScreen";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";

export default function Route() {
  return (
    <RequireCapability need="my_work.read" denied={<NoAccessScreen />}>
      <HubScreen titleKey="nav.group.my_work" groups={["my-work"]} testID="screen-my-work" />
    </RequireCapability>
  );
}
