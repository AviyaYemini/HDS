/** Pay settings: read by pay viewers, edited by admins (specs/007-ux-redesign T083, FR-008). */
import { useCapabilities } from "../../../src/capabilities";
import { PaySettingsScreen } from "../../../src/screens/admin";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";

export default function Route() {
  const capabilities = useCapabilities();
  if (!capabilities.has("pay_settings.edit") && !capabilities.has("pay.read_others")) return <NoAccessScreen />;
  return <PaySettingsScreen />;
}
