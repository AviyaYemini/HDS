/**
 * The root: `Intl` made complete, then strings and direction, the fonts, the theme, the kept
 * session (behind biometrics on a phone), and the routes, with the offline banner, the loading bar
 * and the toasts over everything (specs/007-ux-redesign tasks T015, T016, T021, T022, T040, T044).
 *
 * The web page's static splash (`app/+html.tsx`) stays until the app has started; on phones
 * expo-splash-screen holds the native one for the same stretch.
 */
import "../src/locale/intl";

import { Slot, usePathname, useRouter } from "expo-router";
import * as SplashScreen from "expo-splash-screen";
import { useCallback, useEffect, useRef, useState } from "react";
import { I18nextProvider, useTranslation } from "react-i18next";
import { Platform, StyleSheet, View } from "react-native";
import { SafeAreaProvider, useSafeAreaInsets } from "react-native-safe-area-context";

import { session } from "../src/api/default";
import { SessionProvider } from "../src/api/SessionContext";
import { storeFor } from "../src/api/tokens";
import { unlock } from "../src/auth/biometric";
import { useRetryOnReturn } from "../src/auth/retryOnReturn";
import { markSessionEnded, noteRestore } from "../src/auth/sessionEnded";
import { startAfterRestore, type StartState } from "../src/auth/startState";
import { LoadingBar, ToastHost } from "../src/design/components";
import { useDirectionProps } from "../src/design/components/Screen";
import { startLoading } from "../src/design/loading/loadingStore";
import { useAppFonts } from "../src/design/theme/fonts";
import { ThemeProvider, useTheme } from "../src/design/theme/ThemeProvider";
import { i18n, loadRememberedLanguage, setupI18n } from "../src/i18n";
import { clearResume, skipsUnlock, takeResume } from "../src/i18n/restart";
import { LanguageSwitching } from "../src/navigation/LanguageSwitching";
import { OfflineBanner } from "../src/navigation/OfflineBanner";
import { LockScreen } from "../src/screens/misc/LockScreen";
import { UnreachableScreen } from "../src/screens/misc/UnreachableScreen";

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

type Start = "starting" | "locked" | StartState;

function Shell({
  locked,
  onUnlock,
  onForget,
  unlocking,
  resumeTo,
  onResumed,
}: {
  locked: boolean;
  onUnlock: () => void;
  onForget: () => void;
  unlocking: boolean;
  resumeTo: string | null;
  onResumed: () => void;
}) {
  const { colors } = useTheme();
  const insets = useSafeAreaInsets();
  const direction = useDirectionProps();
  const router = useRouter();
  const pathname = usePathname();
  useRouteLoading();
  // Back to the screen the language switch left (FR-078), before the splash comes down.
  useEffect(() => {
    if (resumeTo === null) return;
    // The web reload kept the address; a phone starts at its first route and is taken back, and
    // only while signed in: a resume is the session's, never shown to whoever signs in next.
    const [path] = resumeTo.split("?");
    if (session.signedIn && (Platform.OS !== "web" || path !== pathname)) router.replace(resumeTo as never);
    onResumed();
  }, [resumeTo, pathname, router, onResumed]);
  return (
    <View {...direction.web} style={[styles.root, direction.style, { backgroundColor: colors.bg.canvas }]}>
      <OfflineBanner />
      {locked ? <LockScreen onUnlock={onUnlock} onSignOut={onForget} busy={unlocking} /> : <Slot />}
      <View pointerEvents="none" style={[styles.bar, { top: insets.top }]}>
        <LoadingBar />
      </View>
      <ToastHost />
      <LanguageSwitching />
    </View>
  );
}

function Starter() {
  const { t } = useTranslation();
  const [start, setStart] = useState<Start>("starting");
  const [unlocking, setUnlocking] = useState(false);

  const [retrying, setRetrying] = useState(false);
  // The screen a language restart left, taken once at the start (FR-078).
  const [resumeTo, setResumeTo] = useState<string | null>(null);
  const resumed = useCallback(() => setResumeTo(null), []);

  /** Take up the kept session, and say what to show: the routes, or the "can't reach HDS" state. */
  const restore = useCallback(async (): Promise<StartState> => {
    const hadTokens = (await storeFor(Platform.OS).load()) !== null;
    const restored = await session.restore();
    noteRestore(hadTokens, restored, session.endedUnexpectedly);
    return startAfterRestore(restored, session.unreachable);
  }, []);

  const retry = useCallback(async (): Promise<void> => {
    setRetrying(true);
    try {
      setStart(await restore());
    } finally {
      setRetrying(false);
    }
  }, [restore]);
  useRetryOnReturn(start === "unreachable", () => void retry());

  const tryUnlock = useCallback(async (): Promise<void> => {
    setUnlocking(true);
    try {
      const result = await unlock(t("lock.prompt"), t("common.cancel"));
      if (result === "cancelled") {
        setStart("locked");
        return;
      }
      setStart(await restore());
    } finally {
      setUnlocking(false);
    }
  }, [restore, t]);

  // The start runs once. `tryUnlock` changes whenever `t` does, so switching the language would
  // otherwise take the kept session up again and ask for biometrics a second time (R72).
  const started = useRef(false);
  useEffect(() => {
    if (started.current) return;
    started.current = true;
    void (async () => {
      try {
        const kept = await storeFor(Platform.OS).load();
        const hadTokens = kept !== null;
        // Taken by every start, used or not, so it never outlives the switch.
        const resume = await takeResume();
        if (resume) setResumeTo(resume.path);
        // A restart for the language, seconds ago, by the very session that was unlocked: not a
        // new start, so no second biometric prompt (the owner: it must never look like a cold
        // start; ADR 016, biometrics are a convenience gate).
        if (hadTokens && Platform.OS !== "web" && !skipsUnlock(resume, kept?.access)) {
          await tryUnlock();
        } else {
          setStart(await restore());
        }
      } catch {
        setStart("ready");
      }
    })();
  }, [restore, tryUnlock]);

  useEffect(() => {
    // Held until the restart's screen is back, so the person never sees the home screen first.
    if (start === "starting" || (resumeTo !== null && start === "ready")) return;
    hideWebSplash();
    void SplashScreen.hideAsync().catch(() => undefined);
  }, [start, resumeTo]);

  useEffect(() => {
    // A refresh the server refused while the app was open: say so on the sign-in screen.
    return session.subscribe(() => {
      if (!session.signedIn && session.endedUnexpectedly) markSessionEnded();
      // Signing out forgets a pending resume, kept and in hand: it belongs to the session that
      // switched (group O review).
      if (!session.signedIn) {
        void clearResume();
        setResumeTo(null);
      }
    });
  }, []);

  if (start === "starting") return null;
  if (start === "unreachable") {
    // Kept tokens, no server: offer a retry rather than the sign-in form (PR 2 review).
    return (
      <UnreachableScreen
        onRetry={() => void retry()}
        onSignIn={() => {
          setResumeTo(null);
          setStart("ready");
        }}
        busy={retrying}
      />
    );
  }
  return (
    <Shell
      locked={start === "locked"}
      unlocking={unlocking}
      resumeTo={start === "ready" ? resumeTo : null}
      onResumed={resumed}
      onUnlock={() => void tryUnlock()}
      onForget={() => {
        setResumeTo(null);
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
