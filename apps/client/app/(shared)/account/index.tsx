/** The Account screen (FR-009). */
import { RequireCapability } from "../../../src/capabilities";
import { AccountScreen } from "../../../src/screens/account";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";

export default function Route() {
  return (
    <RequireCapability need="account.self" denied={<NoAccessScreen />}>
      <AccountScreen />
    </RequireCapability>
  );
}
