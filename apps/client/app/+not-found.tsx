/** Any unknown address: the 404 page, inside the shell when signed in (FR-077). */
import { useSignedIn } from "../src/api/SessionContext";
import { Shell } from "../src/navigation/Shell";
import { NotFoundScreen } from "../src/screens/misc/NotFoundScreen";

export default function NotFound() {
  const signedIn = useSignedIn();
  if (!signedIn) return <NotFoundScreen />;
  return (
    <Shell>
      <NotFoundScreen />
    </Shell>
  );
}
