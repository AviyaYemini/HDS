/**
 * The screens every role shares (account, inbox, no access, help, contact, the accessibility
 * statement), inside whichever shell the person's capabilities give them.
 */
import { Slot } from "expo-router";

import { useSignedIn } from "../../src/api/SessionContext";
import { SignInRedirect } from "../../src/auth/SignInRedirect";
import { Shell } from "../../src/navigation/Shell";

export default function SharedLayout() {
  if (!useSignedIn()) return <SignInRedirect />;
  return (
    <Shell>
      <Slot />
    </Shell>
  );
}
