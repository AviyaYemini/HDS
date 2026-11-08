/**
 * The language switch's restart, as people see it (specs/007-ux-redesign FR-078, T105): the moment
 * the other language is picked, the whole screen says so in that language ("Switching to English…"
 * / "עוברים לעברית…") under the loading bar, laid out in its direction, until the app restarts.
 */
import { useGlobalSearchParams, usePathname, useSegments } from "expo-router";
import { useEffect } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { Txt, showToast } from "../design/components";
import { startLoading } from "../design/loading/loadingStore";
import { useTheme } from "../design/theme/ThemeProvider";
import { webProps } from "../design/web";
import { i18n, isRtlLanguage } from "../i18n";
import { queryOnly } from "../auth/SignInRedirect";
import { restartInLanguage, resumePath, useSwitching } from "../i18n/restart";

/** Switch to `language` from the screen the person is on, by restarting in it. */
export function useLanguageSwitch(): (language: string) => void {
  const { t } = useTranslation();
  const pathname = usePathname();
  const params = useGlobalSearchParams<Record<string, string | string[]>>();
  const segments = useSegments() as string[];
  return (language: string) => {
    if (i18n.language.startsWith(language)) return;
    void restartInLanguage(language, resumePath(pathname, queryOnly(params, segments)), {
      onRestartNeeded: () => showToast(t("language.restart_notice"), { tone: "info", duration: 6000 }),
    });
  };
}

/** Announced at once, and busy until the restart. */
const LIVE = { "aria-live": "assertive", "aria-busy": "true" } as const;

/** Over everything while the language switches; nothing otherwise. */
export function LanguageSwitching() {
  const target = useSwitching();
  const { colors } = useTheme();
  useEffect(() => {
    if (target === null) return undefined;
    return startLoading();
  }, [target]);
  if (target === null) return null;
  // In the language being switched to, before the app has switched (`t` fixed to it).
  const t = i18n.getFixedT(target);
  const dir = isRtlLanguage(target) ? "rtl" : "ltr";
  return (
    <View
      testID="language-switching"
      role="alert"
      {...webProps({ dir, lang: target, ...LIVE })}
      style={[styles.root, { backgroundColor: colors.bg.canvas, direction: dir }]}
    >
      <Txt role="display" dir={dir}>
        {t(`language.switching_to.${target}`)}
      </Txt>
      <Txt role="body" tone="secondary" dir={dir}>
        {t("language.switching_body")}
      </Txt>
    </View>
  );
}

const styles = StyleSheet.create({
  root: { position: "absolute", top: 0, bottom: 0, start: 0, end: 0, zIndex: 1000, alignItems: "center", justifyContent: "center", gap: 12, padding: 24 },
});
