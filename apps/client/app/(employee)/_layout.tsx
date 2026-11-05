/** The signed-in area: a bar with the employee screens and sign-out; sign-in when signed out. */
import { Redirect, Slot } from "expo-router";

import { useSignedIn } from "../../src/api/SessionContext";
import { EmployeeNav } from "../../src/components/EmployeeNav";

export default function EmployeeLayout() {
  if (!useSignedIn()) return <Redirect href="/sign-in" />;
  return (
    <>
      <EmployeeNav />
      <Slot />
    </>
  );
}
