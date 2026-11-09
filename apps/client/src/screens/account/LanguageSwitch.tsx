/**
 * The language switch (specs/007-ux-redesign FR-059): a radio group, applied at once.
 */
import { useTranslation } from "react-i18next";
import { Pressable, View } from "react-native";

import { Txt, focusRing, type PressState } from "../../design/components";
import { useKeyboardMode } from "../../design/focus";
import { useTheme } from "../../design/theme/ThemeProvider";
import { component } from "../../design/theme/theme.generated";
import { webProps } from "../../design/web";
import { useLanguageSwitch } from "../../navigation/LanguageSwitching";
import { styles } from "./accountStyles";

const LANGUAGES = [
  { code: "he", labelKey: "language.he" },
  { code: "en", labelKey: "language.en" },
] as const;

/** The language switch: a radio group, applied at once (FR-059). */
export function LanguageSwitch() {
  const { t, i18n } = useTranslation();
  const { colors } = useTheme();
  const keyboard = useKeyboardMode();
  // The same restart as the top bar's switch (FR-078).
  const choose = useLanguageSwitch();
  return (
    <View role="radiogroup" accessibilityLabel={t("language.label")} style={[styles.radios, { borderColor: colors.border.strong }]}>
      {LANGUAGES.map((language) => {
        const checked = i18n.language.startsWith(language.code);
        return (
          <Pressable
            key={language.code}
            testID={`language-${language.code}`}
            role="radio"
            accessibilityState={{ checked }}
            aria-checked={checked}
            onPress={() => choose(language.code)}
            {...webProps({ tabIndex: checked ? 0 : -1, lang: language.code })}
            style={(state) => [
              styles.radio,
              { minHeight: component.target.phone, borderTopColor: checked ? colors.accent.default : "transparent" },
              checked ? { backgroundColor: colors.bg.selected } : null,
              keyboard && (state as PressState).focused ? focusRing(colors.focus.ring) : null,
            ]}
          >
            <Txt role="label" tone="inherit" style={{ color: checked ? colors.accent.onSelected : colors.text.primary }}>
              {t(language.labelKey)}
            </Txt>
          </Pressable>
        );
      })}
    </View>
  );
}
