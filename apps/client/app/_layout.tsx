/**
 * The root: `Intl` made complete, then strings and direction, the fonts, the theme, the kept
 * session (behind biometrics on a phone), and the routes, with the offline banner, the loading bar
 * and the toasts over everything (specs/007-ux-redesign tasks T015, T016, T021, T022, T040, T044).
 *
 * The web page's static splash (`app/+html.tsx`) stays until the app has started; on phones
 * expo-splash-screen holds the native one for the same stretch.
 */
import "../src/locale/intl";

import { Slot, usePathname } from "expo-router";
import * as SplashScreen from "expo-splash-screen";
import { useCallback, useEffect, useState } from "react";
import { I18nextProvider, useTranslation } from "react-i18next";
import { Platform, StyleSheet, View } from "react-native";
import { SafeAreaProvider, useSafeAreaInsets } from "react-native-safe-area-context";

import { session } from "../src/api/default";
import { SessionProvider } from "../src/api/SessionContext";
import { storeFor } from "../src/api/tokens";
import { unlock } from "../src/auth/biometric";
import { markSessionEnded, noteRestore } from "../src/auth/sessionEnded";
import { LoadingBar, ToastHost } from "../src/design/components";
import { useDirectionProps } from "../src/design/components/Screen";
import { startLoading } from "../src/design/loading/loadingStore";
import { useAppFonts } from "../src/design/theme/fonts";
import { ThemeProvider, useTheme } from "../src/design/theme/ThemeProvider";
import { i18n, loadRememberedLanguage, setupI18n } from "../src/i18n";
import { OfflineBanner } from "../src/navigation/OfflineBanner";
import { LockScreen } from "../src/screens/misc/LockScreen";

void SplashScreen.preventAutoHideAsync().catch(() => undefined);

/** Raise the loading bar for a route change that takes longer than a frame. */
function useRouteLoading(): void {
  const pathname = usePathname();
  useEffect(() => {
    const done = startLoading();
    const handle = requestAnimationFrame(() => setTimeout(done, 0));
    return () => {
      cancelAnimationFrame(handle);
      done();
    };
  }, [pathname]);
}

/** Take the web page's static splash down once the app draws. */
function hideWebSplash(): void {
  if (Platform.OS !== "web" || typeof document === "undefined") return;
  document.getElementById("splash")?.remove();
}

type Start = "starting" | "locked" | "ready";

function Shell({ locked, onUnlock, onForget, unlocking }: { locked: boolean; onUnlock: () => void; onForget: () => void; unlocking: boolean }) {
  const { colors } = useTheme();
  const insets = useSafeAreaInsets();
  const direction = useDirectionProps();
  useRouteLoading();
  return (
    <View {...direction.web} style={[styles.root, direction.style, { backgroundColor: colors.bg.canvas }]}>
      <OfflineBanner />
      {locked ? <LockScreen onUnlock={onUnlock} onSignOut={onForget} busy={unlocking} /> : <Slot />}
      <View pointerEvents="none" style={[styles.bar, { top: insets.top }]}>
        <LoadingBar />
      </View>
      <ToastHost />
    </View>
  );
}

function Starter() {
  const { t } = useTranslation();
  const [start, setStart] = useState<Start>("starting");
  const [unlocking, setUnlocking] = useState(false);

  const restore = useCallback(async (): Promise<void> => {
    const hadTokens = (await storeFor(Platform.OS).load()) !== null;
    const restored = await session.restore();
    noteRestore(hadTokens, restored, session.endedUnexpectedly);
  }, []);

  const tryUnlock = useCallback(async (): Promise<void> => {
    setUnlocking(true);
    try {
      const result = await unlock(t("lock.prompt"), t("common.cancel"));
      if (result === "cancelled") {
        setStart("locked");
        return;
      }
      await restore();
      setStart("ready");
    } finally {
      setUnlocking(false);
    }
  }, [restore, t]);

  useEffect(() => {
    void (async () => {
      try {
        const hadTokens = (await storeFor(Platform.OS).load()) !== null;
        if (hadTokens && Platform.OS !== "web") {
          await tryUnlock();
        } else {
          await restore();
          setStart("ready");
        }
      } catch {
        setStart("ready");
      }
    })();
  }, [restore, tryUnlock]);

  useEffect(() => {
    if (start === "starting") return;
    hideWebSplash();
    void SplashScreen.hideAsync().catch(() => undefined);
  }, [start]);

  useEffect(() => {
    // A refresh the server refused while the app was open: say so on the sign-in screen.
    return session.subscribe(() => {
      if (!session.signedIn && session.endedUnexpectedly) markSessionEnded();
    });
  }, []);

  if (start === "starting") return null;
  return (
    <Shell
      locked={start === "locked"}
      unlocking={unlocking}
      onUnlock={() => void tryUnlock()}
      onForget={() => {
        void session.signOut().then(() => setStart("ready"));
      }}
    />
  );
}

export default function RootLayout() {
  const [strings, setStrings] = useState(false);
  const fontsReady = useAppFonts();

  useEffect(() => {
    void (async () => {
      try {
        await loadRememberedLanguage();
        await setupI18n();
      } catch {
        // English is bundled, so the screens still have strings.
      } finally {
        setStrings(true);
      }
    })();
  }, []);

  if (!strings || !fontsReady) return null;
  return (
    <SafeAreaProvider>
      <I18nextProvider i18n={i18n}>
        <ThemeProvider>
          <SessionProvider session={session}>
            <Starter />
          </SessionProvider>
        </ThemeProvider>
      </I18nextProvider>
    </SafeAreaProvider>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1 },
  bar: { position: "absolute", start: 0, end: 0 },
});
