/**
 * A titled section of the Account screen (specs/007-ux-redesign FR-009).
 */
import { useTranslation } from "react-i18next";
import { View } from "react-native";

import { Txt } from "../../design/components";
import { useTheme } from "../../design/theme/ThemeProvider";
import { styles } from "./accountStyles";

export function Section({ titleKey, children, testID }: { titleKey: string; children: React.ReactNode; testID?: string }) {
  const { t } = useTranslation();
  const { colors } = useTheme();
  return (
    <View testID={testID} style={[styles.section, { borderTopColor: colors.border.subtle }]}>
      <Txt role="section" heading={2}>
        {t(titleKey)}
      </Txt>
      {children}
    </View>
  );
}
