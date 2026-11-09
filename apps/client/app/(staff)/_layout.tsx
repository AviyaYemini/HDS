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
import { RunFinishedNotice } from "../../src/screens/runs/RunFinishedNotice";

export default function StaffLayout() {
  const signedIn = useSignedIn();
  const capabilities = useCapabilities();
  if (!signedIn) return <SignInRedirect />;
  if (shellFor(capabilities) === "client") return <Redirect href="/no-access" />;
  const runs = capabilities.has("schedule.generate");
  return (
    <Shell>
      <Slot />
      {/* A run someone started tells them when it ends, wherever they are (008 FR-029). */}
      {runs ? <RunFinishedNotice /> : null}
    </Shell>
  );
}
