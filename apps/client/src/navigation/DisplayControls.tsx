/**
 * The language and light/dark controls in the top bar (the owner, PR 4): the language switch shows
 * the language it switches to, by its own name, and the theme toggle the mode it switches to.
 * Account keeps the full settings (high contrast, following the device); these are the shortcuts.
 */
import { useTranslation } from "react-i18next";
import { Pressable, StyleSheet } from "react-native";

import { Txt, focusRing, type PressState } from "../design/components";
import { useKeyboardMode } from "../design/focus";
import { Icon, type IconName } from "../design/icons/Icon";
import { useTheme } from "../design/theme/ThemeProvider";
import type { ThemeName } from "../design/theme/theme.generated";
import { component } from "../design/theme/theme.generated";
import { webProps } from "../design/web";
import { useLanguageSwitch } from "./LanguageSwitching";

/** The theme the toggle switches to from `current`: light ↔ dark, high contrast kept. */
export function toggledTheme(current: ThemeName): ThemeName {
  switch (current) {
    case "light":
      return "dark";
    case "dark":
      return "light";
    case "highContrastLight":
      return "highContrastDark";
    case "highContrastDark":
      return "highContrastLight";
  }
}

/** The language the switch offers from `current`. */
export function otherLanguage(current: string): "he" | "en" {
  return current.startsWith("he") ? "en" : "he";
}

function ControlButton({
  icon,
  label,
  accessibilityLabel,
  testID,
  onPress,
  lang,
}: {
  icon: IconName;
  label: string;
  accessibilityLabel: string;
  testID: string;
  onPress: () => void;
  lang?: string;
}) {
  const { colors, isDesktop } = useTheme();
  const keyboard = useKeyboardMode();
  return (
    <Pressable
      role="button"
      accessibilityLabel={accessibilityLabel}
      testID={testID}
      onPress={onPress}
      {...webProps(lang ? { lang } : {})}
      style={(state: PressState) => [
        styles.button,
        (state.hovered || state.pressed) && { backgroundColor: colors.bg.selected },
        keyboard && state.focused ? focusRing(colors.focus.ring) : null,
      ]}
    >
      <Icon name={icon} size={20} color={colors.text.primary} />
      {isDesktop ? (
        <Txt role="label" numberOfLines={1}>
          {label}
        </Txt>
      ) : null}
    </Pressable>
  );
}

/** Switch to the other language, named in itself ("English" from Hebrew, "עברית" from English). */
export function LanguageToggle() {
  const { t, i18n } = useTranslation();
  const switchTo = useLanguageSwitch();
  const target = otherLanguage(i18n.language);
  const name = t(`language.${target}`);
  return (
    <ControlButton
      icon="globe"
      label={name}
      accessibilityLabel={t("display.language_to", { language: name })}
      lang={target}
      testID="action-switch-language"
      onPress={() => switchTo(target)}
    />
  );
}

/** Switch light ↔ dark; the choice is kept, as Account's is. */
export function ThemeToggle() {
  const { t } = useTranslation();
  const theme = useTheme();
  const next = toggledTheme(theme.name);
  const toDark = next === "dark" || next === "highContrastDark";
  const label = toDark ? t("display.dark_mode") : t("display.light_mode");
  return (
    <ControlButton
      icon={toDark ? "moon" : "sun"}
      label={label}
      accessibilityLabel={label}
      testID="action-toggle-theme"
      onPress={() => theme.setPreference(next)}
    />
  );
}

const styles = StyleSheet.create({
  button: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    minHeight: component.target.phone,
    minWidth: component.target.phone,
    paddingHorizontal: 10,
    justifyContent: "center",
  },
});
