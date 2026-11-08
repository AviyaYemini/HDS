/** Pay settings: built by group F; read by pay viewers, edited by admins (FR-008). */
import { useCapabilities } from "../../../src/capabilities";
import { PlaceholderScreen } from "../../../src/screens/misc/HubScreen";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";

export default function Route() {
  const capabilities = useCapabilities();
  if (!capabilities.has("pay_settings.edit") && !capabilities.has("pay.read_others")) return <NoAccessScreen />;
  return <PlaceholderScreen titleKey="nav.pay_settings" bodyKey="placeholder.admin" eyebrowKey="nav.group.admin" testID="screen-admin-pay-settings" />;
}
