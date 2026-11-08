/**
 * Hubs and placeholders. A hub is a phone tab that gathers several destinations (My work, Reports,
 * More); a placeholder stands where a later group of 007 builds the screen (the roster, runs, the
 * admin tables), so no destination is a dead end (FR-007) and the person is told what is coming.
 */
import { useRouter } from "expo-router";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { useSession } from "../../api/SessionContext";
import { useCapabilities } from "../../capabilities";
import { Button, LinkPressable, PageTitle, Txt, focusRing } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { useKeyboardMode } from "../../design/focus";
import { Icon } from "../../design/icons/Icon";
import { useTheme } from "../../design/theme/ThemeProvider";
import { component } from "../../design/theme/theme.generated";
import { webProps } from "../../design/web";
import { destinationsFor, type Destination, type NavGroup } from "../../navigation/destinations";

/** One destination as a row with its icon and label. */
export function DestinationRow({ item }: { item: Destination }) {
  const { t } = useTranslation();
  const { colors } = useTheme();
  const keyboard = useKeyboardMode();
  const label = t(item.labelKey);
  const webOnly = webProps(item.preview ? { dataSet: { preview: "true" } } : {});
  return (
    <LinkPressable
      href={item.href}
      role="link"
      testID={`hub-${item.key}`}
      {...webOnly}
      style={(state) => [
        styles.row,
        { borderBottomColor: colors.border.subtle, minHeight: component.target.phone + 8 },
        state.hovered || state.pressed ? { backgroundColor: colors.bg.surface } : null,
        keyboard && state.focused ? focusRing(colors.focus.ring) : null,
      ]}
    >
        <Icon name={item.icon} size={20} color={colors.text.secondary} />
        <Txt role="label" style={styles.rowLabel}>
          {label}
        </Txt>
        {item.preview ? (
          <Txt role="eyebrow" tone="secondary">
            {t("preview.badge")}
          </Txt>
        ) : null}
        <Icon name="chevron-next" size={18} color={colors.text.secondary} />
    </LinkPressable>
  );
}

/** A hub of the destinations in `groups` the person may open. */
export function HubScreen({ titleKey, groups, testID, children }: { titleKey: string; groups: readonly NavGroup[]; testID: string; children?: React.ReactNode }) {
  const { t } = useTranslation();
  const capabilities = useCapabilities();
  const items = destinationsFor(capabilities).filter((item) => groups.includes(item.group) && item.key !== "home");
  return (
    <Screen testID={testID}>
      <PageTitle eyebrow={[t("app.name"), t(titleKey)]} title={t(titleKey)} />
      <View style={styles.list}>
        {items.map((item) => (
          <DestinationRow key={item.key} item={item} />
        ))}
      </View>
      {children}
    </Screen>
  );
}

/** The phone's More tab: the admin screens the person has, the account and the links. */
export function MoreScreen() {
  const { t } = useTranslation();
  const router = useRouter();
  const session = useSession();
  const signOut = async (): Promise<void> => {
    await session.signOut();
    router.replace("/sign-in");
  };
  return (
    <HubScreen titleKey="nav.more" groups={["admin"]} testID="screen-more">
      <View style={styles.list}>
        <DestinationRow item={{ key: "account", href: "/account", labelKey: "nav.account", icon: "user", group: "my-work", need: ["account.self"] }} />
      </View>
      <View style={styles.actions}>
        <Button variant="secondary" label={t("auth.sign_out")} icon="sign-out" onPress={() => void signOut()} testID="action-sign-out-more" />
      </View>
    </HubScreen>
  );
}

const styles = StyleSheet.create({
  list: { alignSelf: "stretch" },
  row: { flexDirection: "row", alignItems: "center", gap: 12, paddingVertical: 10, paddingHorizontal: 4, borderBottomWidth: 1 },
  rowLabel: { flex: 1 },
  actions: { flexDirection: "row", gap: 12, marginTop: 8 },
});
