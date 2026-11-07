/**
 * The root: `Intl` made complete, then strings and direction, the fonts, the theme, the kept
 * session, and the routes, with the loading bar and the toasts over everything
 * (specs/007-ux-redesign tasks T015, T016, T021, T022).
 */
import "../src/locale/intl";

import { Slot, usePathname } from "expo-router";
import { useEffect, useState } from "react";
import { I18nextProvider } from "react-i18next";
import { StyleSheet, View } from "react-native";
import { SafeAreaProvider, useSafeAreaInsets } from "react-native-safe-area-context";

import { session } from "../src/api/default";
import { SessionProvider } from "../src/api/SessionContext";
import { LoadingBar, ToastHost } from "../src/design/components";
import { useDirectionProps } from "../src/design/components/Screen";
import { startLoading } from "../src/design/loading/loadingStore";
import { useAppFonts } from "../src/design/theme/fonts";
import { ThemeProvider, useTheme } from "../src/design/theme/ThemeProvider";
import { i18n, setupI18n } from "../src/i18n";

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

function Shell() {
  const { colors } = useTheme();
  const insets = useSafeAreaInsets();
  const direction = useDirectionProps();
  useRouteLoading();
  return (
    <View {...direction.web} style={[styles.root, direction.style, { backgroundColor: colors.bg.canvas }]}>
      <Slot />
      <View pointerEvents="none" style={[styles.bar, { top: insets.top }]}>
        <LoadingBar />
      </View>
      <ToastHost />
    </View>
  );
}

export default function RootLayout() {
  const [ready, setReady] = useState(false);
  const fontsReady = useAppFonts();

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

  if (!ready || !fontsReady) return null;
  return (
    <SafeAreaProvider>
      <I18nextProvider i18n={i18n}>
        <ThemeProvider>
          <SessionProvider session={session}>
            <Shell />
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
