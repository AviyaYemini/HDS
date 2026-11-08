/** The start: the role's home when signed in, else sign-in. */
import { Redirect } from "expo-router";

import { useSignedIn } from "../src/api/SessionContext";
import { useCapabilities } from "../src/capabilities";
import { homeFor } from "../src/navigation/destinations";

export default function Index() {
  const signedIn = useSignedIn();
  const capabilities = useCapabilities();
  return <Redirect href={(signedIn ? homeFor(capabilities) : "/sign-in") as never} />;
}
