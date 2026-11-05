/** The start: My availability when signed in, else sign-in. */
import { Redirect } from "expo-router";

import { useSignedIn } from "../src/api/SessionContext";

export default function Index() {
  return <Redirect href={useSignedIn() ? "/availability" : "/sign-in"} />;
}
