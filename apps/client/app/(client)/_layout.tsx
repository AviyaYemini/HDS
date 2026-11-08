/**
 * The client shell (specs/007-ux-redesign FR-004, task T036): Projects, Billing, Requests and
 * Contact, and never a staff destination. Staff who open a client address meet the no-access page.
 */
import { Slot } from "expo-router";

import { useSignedIn } from "../../src/api/SessionContext";
import { SignInRedirect } from "../../src/auth/SignInRedirect";
import { RequireCapability } from "../../src/capabilities";
import { Shell } from "../../src/navigation/Shell";
import { NoAccessScreen } from "../../src/screens/no-access/NoAccessScreen";

export default function ClientLayout() {
  const signedIn = useSignedIn();
  if (!signedIn) return <SignInRedirect />;
  return (
    <Shell>
      <RequireCapability need="client_portal.read" denied={<NoAccessScreen />}>
        <Slot />
      </RequireCapability>
    </Shell>
  );
}
