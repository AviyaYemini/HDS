/**
 * A page's title with its eyebrow breadcrumb, "HDS / SCHEDULING" above the display text
 * (docs/plans/ux-working-plan.md section 3; specs/007-ux-redesign FR-061).
 *
 * Actions sit on the trailing side on a desktop window and under the title on a phone.
 */
import { useEffect, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Platform, StyleSheet, View } from "react-native";

import { useTheme } from "../theme/ThemeProvider";
import { Txt } from "./Text";

export interface PageTitleProps {
  /** The crumbs, joined with " / ". */
  eyebrow: readonly string[];
  title: string;
  /** Buttons or chips beside the title. */
  children?: ReactNode;
  /** The browser tab's name when the visible title is not it (a month, a week); the title by default. */
  documentTitle?: string;
  testID?: string;
}

/** A page title. */
export function PageTitle({ eyebrow, title, children, documentTitle, testID = "page-title" }: PageTitleProps) {
  const { isDesktop } = useTheme();
  useDocumentTitle(documentTitle ?? title);
  return (
    <View testID={testID ? `${testID}-root` : undefined} style={[styles.root, isDesktop ? styles.rootDesktop : null]}>
      <View style={styles.text}>
        <Txt role="eyebrow" tone="secondary" testID={testID ? `${testID}-eyebrow` : undefined}>
          {eyebrow.join(" / ")}
        </Txt>
        <Txt role="display" heading={1} testID={testID}>
          {title}
        </Txt>
      </View>
      {children ? <View style={styles.actions}>{children}</View> : null}
    </View>
  );
}

const styles = StyleSheet.create({
  root: { gap: 16 },
  rootDesktop: { flexDirection: "row", alignItems: "flex-end", justifyContent: "space-between" },
  text: { gap: 8, flexShrink: 1 },
  actions: { flexDirection: "row", flexWrap: "wrap", gap: 12, alignItems: "center" },
});

/** The browser tab's title on the web: "<name> · HDS". */
export function useDocumentTitle(name: string): void {
  const { t } = useTranslation();
  const app = t("app.name");
  useEffect(() => {
    if (Platform.OS === "web" && typeof document !== "undefined") document.title = `${name} · ${app}`;
  }, [name, app]);
}
