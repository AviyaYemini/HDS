/** The root: strings and direction, then the kept session, then the routes. */
import { Slot } from "expo-router";
import { useEffect, useState } from "react";
import { I18nextProvider } from "react-i18next";

import { session } from "../src/api/default";
import { SessionProvider } from "../src/api/SessionContext";
import { i18n, setupI18n } from "../src/i18n";

export default function RootLayout() {
  const [ready, setReady] = useState(false);

  useEffect(() => {
    void (async () => {
      try {
        await setupI18n();
        await session.restore();
      } catch {
        // English is bundled, so the screens still have strings; the session starts signed out.
      } finally {
        setReady(true);
      }
    })();
  }, []);

  if (!ready) return null;
  return (
    <I18nextProvider i18n={i18n}>
      <SessionProvider session={session}>
        <Slot />
      </SessionProvider>
    </I18nextProvider>
  );
}
