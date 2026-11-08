/**
 * The staff shell (specs/007-ux-redesign FR-003, FR-005, task T035): everyone with "My work", the
 * "Manage" group for managers and admins, the admin-only screens for admins, one navigation and no
 * profile switch. Signed-out visitors go to sign-in; a client goes to the client portal.
 */
import { Redirect, Slot } from "expo-router";

import { useSignedIn } from "../../src/api/SessionContext";
import { SignInRedirect } from "../../src/auth/SignInRedirect";
import { useCapabilities } from "../../src/capabilities";
import { Shell } from "../../src/navigation/Shell";
import { shellFor } from "../../src/navigation/destinations";

export default function StaffLayout() {
  const signedIn = useSignedIn();
  const capabilities = useCapabilities();
  if (!signedIn) return <SignInRedirect />;
  if (shellFor(capabilities) === "client") return <Redirect href="/no-access" />;
  return (
    <Shell>
      <Slot />
    </Shell>
  );
}
