/**
 * An empty state (specs/007-ux-redesign FR-048, FR-063, FR-068): what this place is for, then one
 * next action. Preview stubs of later features use the `feature` line to say which feature brings
 * the screen.
 */
import { StyleSheet, View, type StyleProp, type ViewStyle } from "react-native";

import { Icon, type IconName } from "../icons/Icon";
import { useTheme } from "../theme/ThemeProvider";
import { Button, type ButtonVariant } from "./Button";
import { StatusChip } from "./StatusChip";
import { Txt } from "./Text";

export interface EmptyStateProps {
  title: string;
  body?: string;
  icon?: IconName;
  /** The one action; `primary` makes it the screen's main action. */
  action?: { label: string; onPress: () => void; variant?: ButtonVariant; testID?: string };
  /** A chip above the title, such as "Preview" with the feature's name. */
  badge?: string;
  /** A meta line under the body, such as "Comes with feature 009". */
  feature?: string;
  testID?: string;
  style?: StyleProp<ViewStyle>;
}

/** An empty state. */
export function EmptyState({ title, body, icon, action, badge, feature, testID, style }: EmptyStateProps) {
  const { colors } = useTheme();
  return (
    <View testID={testID} style={[styles.root, { borderColor: colors.border.subtle }, style]}>
      {badge ? <StatusChip status="neutral" label={badge} /> : null}
      {icon ? <Icon name={icon} size={28} color={colors.text.secondary} /> : null}
      <Txt role="section" heading={2} style={styles.center}>
        {title}
      </Txt>
      {body ? (
        <Txt role="body" tone="secondary" style={styles.center}>
          {body}
        </Txt>
      ) : null}
      {feature ? (
        <Txt role="meta" tone="secondary" style={styles.center}>
          {feature}
        </Txt>
      ) : null}
      {action ? (
        <Button label={action.label} onPress={action.onPress} variant={action.variant ?? "secondary"} testID={action.testID} />
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  root: {
    alignItems: "center",
    gap: 12,
    paddingVertical: 40,
    paddingHorizontal: 24,
    borderWidth: 1,
    borderStyle: "dashed",
    maxWidth: 480,
    alignSelf: "center",
    width: "100%",
  },
  center: { textAlign: "center" },
});
