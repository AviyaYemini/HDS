/**
 * The shells (specs/007-ux-redesign FR-003 to FR-005, FR-009; docs/plans/ux-working-plan.md
 * section 3): a 64 px header whose active item has a 2 px accent rule on `bg.selected`, a side
 * navigation on a desktop window (on the right in Hebrew) and a 60 px bottom bar of at most five
 * tabs on a phone, both drawn from the capability map. Sign-out, help, contact and the
 * accessibility statement sit in the same place on every screen: sign-out in the header, the three
 * links at the foot of every page.
 *
 * The shell owns the one scroll view, so a page inside it renders flat (`Screen` notices the shell
 * and drops its own scrolling) and the footer follows the content.
 */
import { Link, usePathname, useRouter } from "expo-router";
import { createContext, useContext, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Pressable, ScrollView, StyleSheet, View, type ViewStyle } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { useSession } from "../api/SessionContext";
import { useCapabilities } from "../capabilities";
import { previewsEnabled } from "../config/previews";
import { Button, LinkPressable, Txt, focusRing, type PressState } from "../design/components";
import { useDirectionProps } from "../design/components/Screen";
import { useKeyboardMode } from "../design/focus";
import { Icon, type IconName } from "../design/icons/Icon";
import { useTheme } from "../design/theme/ThemeProvider";
import { component } from "../design/theme/theme.generated";
import { webProps } from "../design/web";
import {
  groupsFor,
  homeFor,
  isActiveDestination,
  isActiveTab,
  shellFor,
  tabsFor,
  type Destination,
  type NavGroup,
  type Tab,
} from "./destinations";
import { LanguageToggle, ThemeToggle } from "./DisplayControls";

/** Whether the tree is inside a shell, so pages render flat. */
export const ShellContext = createContext(false);

/** Whether this screen is inside a shell. */
export function useInShell(): boolean {
  return useContext(ShellContext);
}

const GROUP_KEYS: Record<NavGroup, string> = {
  "my-work": "nav.group.my_work",
  manage: "nav.group.manage",
  admin: "nav.group.admin",
  client: "nav.group.client",
};

function NavItem({ item, active, phone }: { item: Destination | Tab; active: boolean; phone: boolean }) {
  const { t } = useTranslation();
  const { colors } = useTheme();
  const keyboard = useKeyboardMode();
  const preview = "preview" in item && item.preview === true;
  const testID = `${phone ? "tab" : "nav"}-${item.key}`;
  const label = t(item.labelKey);
  const webOnly = webProps(preview ? { dataSet: { preview: "true" } } : {});
  return (
    <LinkPressable
        href={item.href}
        testID={testID}
        role="link"
        accessibilityState={{ selected: active }}
        aria-current={active ? "page" : undefined}
        {...webOnly}
        style={(state) => {
          const s = state as PressState;
          const base: ViewStyle = phone
            ? {
                flex: 1,
                minWidth: 0,
                overflow: "hidden",
                alignItems: "center",
                justifyContent: "center",
                gap: 2,
                paddingHorizontal: 2,
                minHeight: component.tabBar.height,
                borderTopWidth: component.header.activeRule,
                borderTopColor: active ? colors.accent.default : "transparent",
              }
            : {
                flexDirection: "row",
                alignItems: "center",
                gap: 10,
                minHeight: 40,
                paddingHorizontal: 12,
                borderStartWidth: component.header.activeRule,
                borderStartColor: active ? colors.accent.default : "transparent",
              };
          return [
            base,
            active ? { backgroundColor: colors.bg.selected } : null,
            !active && (s.hovered || s.pressed) ? { backgroundColor: colors.bg.surface } : null,
            keyboard && s.focused ? focusRing(colors.focus.ring) : null,
          ];
        }}
      >
        <Icon name={item.icon} size={phone ? 22 : 18} color={active ? colors.accent.onSelected : colors.text.secondary} />
        <Txt
          role={phone ? "eyebrow" : "label"}
          tone="inherit"
          numberOfLines={1}
          ellipsizeMode="tail"
          style={[{ color: active ? colors.accent.onSelected : colors.text.primary }, phone ? styles.tabLabel : styles.navLabel]}
        >
          {label}
        </Txt>
        {preview && !phone ? (
          <Txt role="eyebrow" tone="secondary" style={styles.previewMark}>
            {t("preview.badge")}
          </Txt>
        ) : null}
    </LinkPressable>
  );
}

function SideNav() {
  const { t } = useTranslation();
  const { colors } = useTheme();
  const capabilities = useCapabilities();
  const pathname = usePathname();
  const groups = groupsFor(capabilities);
  return (
    <View
      testID="nav-side"
      role="navigation"
      accessibilityLabel={t("nav.main")}
      style={[styles.side, { borderEndColor: colors.border.subtle, backgroundColor: colors.bg.canvas }]}
    >
      <ScrollView contentContainerStyle={styles.sideContent}>
        {groups.map(({ group, items }) => (
          <View key={group} testID={`nav-group-${group}`} style={styles.group}>
            {groups.length > 1 || group === "client" ? (
              <Txt role="eyebrow" tone="secondary" style={styles.groupTitle}>
                {t(GROUP_KEYS[group])}
              </Txt>
            ) : null}
            {items.map((item) => (
              <NavItem key={item.key} item={item} active={isActiveDestination(item, pathname)} phone={false} />
            ))}
          </View>
        ))}
      </ScrollView>
    </View>
  );
}

function TabBar() {
  const { t } = useTranslation();
  const { colors } = useTheme();
  const insets = useSafeAreaInsets();
  const capabilities = useCapabilities();
  const pathname = usePathname();
  const tabs = tabsFor(capabilities);
  return (
    <View
      testID="nav-tabs"
      role="navigation"
      accessibilityLabel={t("nav.main")}
      style={[
        styles.tabs,
        { borderTopColor: colors.border.strong, backgroundColor: colors.bg.canvas, paddingBottom: insets.bottom },
      ]}
    >
      {tabs.map((tab) => (
        <NavItem key={tab.key} item={tab} active={isActiveTab(tab, pathname)} phone />
      ))}
    </View>
  );
}

function HeaderButton({ icon, label, href, testID, onPress, preview }: { icon: IconName; label: string; href?: string; testID: string; onPress?: () => void; preview?: boolean }) {
  const { colors, isDesktop } = useTheme();
  const keyboard = useKeyboardMode();
  const webOnly = webProps(preview ? { dataSet: { preview: "true" } } : {});
  const inner = (state: PressState) => [
    styles.headerButton,
    (state.hovered || state.pressed) && { backgroundColor: colors.bg.selected },
    keyboard && state.focused ? focusRing(colors.focus.ring) : null,
  ];
  const content = (
    <>
      <Icon name={icon} size={20} color={colors.text.primary} />
      {isDesktop ? (
        <Txt role="label" numberOfLines={1}>
          {label}
        </Txt>
      ) : null}
    </>
  );
  if (href) {
    return (
      <LinkPressable href={href} testID={testID} role="link" accessibilityLabel={label} {...webOnly} style={(state) => inner(state)}>
        {content}
      </LinkPressable>
    );
  }
  return (
    <Pressable testID={testID} role="button" accessibilityLabel={label} onPress={onPress} {...webOnly} style={(state) => inner(state as PressState)}>
      {content}
    </Pressable>
  );
}

function Header() {
  const { t } = useTranslation();
  const { colors } = useTheme();
  const insets = useSafeAreaInsets();
  const router = useRouter();
  const session = useSession();
  const capabilities = useCapabilities();
  const signOut = async (): Promise<void> => {
    await session.signOut();
    router.replace("/sign-in");
  };
  return (
    <View
      role="banner"
      style={[
        styles.header,
        { paddingTop: insets.top, height: component.header.height + insets.top, borderBottomColor: colors.border.strong, backgroundColor: colors.bg.canvas },
      ]}
    >
      <Link href={homeFor(capabilities) as never} asChild>
        <Pressable role="link" accessibilityLabel={t("app.name")} testID="link-home" style={styles.brand}>
          <View style={[styles.brandMark, { backgroundColor: colors.cta.bg }]} />
          <Txt role="eyebrow" tone="primary">
            {t("app.name")}
          </Txt>
        </Pressable>
      </Link>
      <View style={styles.headerActions}>
        {previewsEnabled ? <HeaderButton icon="inbox" label={t("nav.inbox")} href="/inbox" testID="link-inbox" preview /> : null}
        <LanguageToggle />
        <ThemeToggle />
        <HeaderButton icon="user" label={t("nav.account")} href="/account" testID="link-account" />
        <HeaderButton icon="sign-out" label={t("auth.sign_out")} testID="action-sign-out" onPress={() => void signOut()} />
      </View>
    </View>
  );
}

/** The three links every screen carries (FR-009, X10, L15). */
export function Footer() {
  const { t } = useTranslation();
  const { colors } = useTheme();
  const capabilities = useCapabilities();
  const contact = shellFor(capabilities) === "client" ? "/client/contact" : "/contact";
  return (
    <View role="contentinfo" style={[styles.footer, { borderTopColor: colors.border.subtle }]}>
      <Link href="/help" asChild>
        <Button variant="link" label={t("nav.help")} icon="help" testID="link-help" />
      </Link>
      <Link href={contact as never} asChild>
        <Button variant="link" label={t("nav.contact")} icon="phone" testID="link-contact" />
      </Link>
      <Link href="/accessibility" asChild>
        <Button variant="link" label={t("nav.accessibility")} icon="accessibility" testID="link-accessibility" />
      </Link>
    </View>
  );
}

/** The shell around every signed-in screen. */
export function Shell({ children }: { children: ReactNode }) {
  const { colors, isDesktop, spacing } = useTheme();
  const direction = useDirectionProps();
  const capabilities = useCapabilities();
  const phone = !isDesktop;
  const gutter = isDesktop ? 32 : spacing.gutter;
  return (
    <View {...direction.web} style={[styles.root, direction.style, { backgroundColor: colors.bg.canvas }]}>
      <Header />
      <View style={styles.body}>
        {isDesktop && shellFor(capabilities) !== "none" ? <SideNav /> : null}
        <ScrollView style={styles.main} contentContainerStyle={styles.mainContent} keyboardShouldPersistTaps="handled">
          <View role="main" style={[styles.page, { paddingHorizontal: gutter }]}>
            <ShellContext.Provider value>{children}</ShellContext.Provider>
          </View>
          <View style={[styles.footerWrap, { paddingHorizontal: gutter }]}>
            <Footer />
          </View>
        </ScrollView>
      </View>
      {phone && shellFor(capabilities) !== "none" ? <TabBar /> : null}
    </View>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1 },
  header: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", paddingHorizontal: 16, borderBottomWidth: 1 },
  brand: { flexDirection: "row", alignItems: "center", gap: 10, minHeight: component.target.phone, paddingEnd: 8 },
  brandMark: { width: 14, height: 14 },
  headerActions: { flexDirection: "row", alignItems: "center", gap: 4 },
  headerButton: { flexDirection: "row", alignItems: "center", gap: 8, minHeight: component.target.phone, minWidth: component.target.phone, paddingHorizontal: 10, justifyContent: "center" },
  body: { flex: 1, flexDirection: "row" },
  side: { width: 248, borderEndWidth: 1 },
  sideContent: { paddingVertical: 16, gap: 20 },
  group: { gap: 2 },
  groupTitle: { paddingHorizontal: 14, paddingBottom: 6 },
  previewMark: { marginStart: "auto" },
  // A fifth of a phone (FR-005): a size smaller than the eyebrow, so "AVAILABILITY" fits whole.
  tabLabel: { maxWidth: "100%", textAlign: "center", letterSpacing: -0.2, fontSize: 10 },
  navLabel: { flexShrink: 1 },
  main: { flex: 1 },
  mainContent: { flexGrow: 1 },
  page: { flexGrow: 1, paddingTop: 24, paddingBottom: 24, maxWidth: 78 * 16, width: "100%", alignSelf: "center", gap: 24 },
  footerWrap: { maxWidth: 78 * 16, width: "100%", alignSelf: "center", paddingBottom: 24 },
  footer: { flexDirection: "row", flexWrap: "wrap", gap: 16, paddingTop: 16, borderTopWidth: 1 },
  tabs: { flexDirection: "row", borderTopWidth: 1 },
});
