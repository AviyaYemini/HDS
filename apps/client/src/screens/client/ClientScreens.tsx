/**
 * The client portal's screens for 007 (specs/007-ux-redesign FR-004, FR-012; C1, C6, O5): one card
 * per project the client owns, a project page, and the company's contact card. Nothing here shows
 * an employee field; coverage and billing come with feature 009 as previews.
 */
import { Link, useLocalSearchParams } from "expo-router";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { previewsEnabled } from "../../config/previews";
import { Button, EmptyState, InlineBanner, LinkPressable, PageTitle, SkeletonRows, StatusChip, Txt, focusRing } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { useKeyboardMode } from "../../design/focus";
import { Icon } from "../../design/icons/Icon";
import { useTheme } from "../../design/theme/ThemeProvider";
import { formatNumericDate, isolateLtr, weekdayNames } from "../../locale";
import { useLoad } from "../useLoad";

/** The client's home: one card per project. */
export function ClientHomeScreen() {
  const { t } = useTranslation();
  const { api } = useSession();
  const { colors } = useTheme();
  const keyboard = useKeyboardMode();
  const errorText = useErrorText();
  const projects = useLoad(() => call(api.GET("/api/v1/projects")));
  return (
    <Screen testID="screen-client-home">
      <PageTitle eyebrow={[t("app.name"), t("nav.group.client")]} title={t("client.projects_title")} />
      {projects.error ? (
        <InlineBanner tone="warning" title={errorText(projects.error)} action={{ label: t("common.retry"), onPress: () => void projects.reload() }} />
      ) : projects.data === null ? (
        <SkeletonRows rows={3} testID="client-projects-loading" />
      ) : projects.data.items.length === 0 ? (
        <EmptyState icon="layers" title={t("client.no_projects_title")} body={t("client.no_projects_body")} />
      ) : (
        <View style={styles.cards}>
          {projects.data.items.map((project) => (
            <LinkPressable
              key={project.id}
              href={`/client/projects/${project.id}`}
              role="link"
              testID={`client-project-${project.id}`}
              style={(state) => [
                styles.card,
                { borderColor: colors.border.subtle, backgroundColor: colors.bg.surface },
                state.hovered || state.pressed ? { borderColor: colors.border.strong } : null,
                keyboard && state.focused ? focusRing(colors.focus.ring) : null,
              ]}
            >
                <View style={styles.cardHead}>
                  <Txt role="section" heading={2} style={styles.cardTitle}>
                    {project.name}
                  </Txt>
                  <StatusChip status={project.status === "active" ? "success" : "neutral"} label={t(`project.status.${project.status}`)} />
                </View>
                <Txt role="meta" tone="secondary" numeric>
                  {project.end_date
                    ? `${formatNumericDate(project.start_date)} – ${formatNumericDate(project.end_date)}`
                    : t("client.since", { date: formatNumericDate(project.start_date) })}
                </Txt>
                <Txt role="meta" tone="secondary">
                  {t(`project.type.${project.type}`)}
                </Txt>
            </LinkPressable>
          ))}
        </View>
      )}
    </Screen>
  );
}

/** One project, as the client sees it. */
export function ClientProjectScreen() {
  const { t } = useTranslation();
  const { id } = useLocalSearchParams<{ id: string }>();
  const { api } = useSession();
  const { colors } = useTheme();
  const errorText = useErrorText();
  const project = useLoad(() => call(api.GET("/api/v1/projects/{id}", { params: { path: { id: String(id) } } })), [id]);
  return (
    <Screen testID="screen-client-project">
      {project.error ? (
        <InlineBanner tone="warning" title={errorText(project.error)} action={{ label: t("common.retry"), onPress: () => void project.reload() }} />
      ) : project.data === null ? (
        <SkeletonRows rows={3} />
      ) : (
        <>
          <PageTitle eyebrow={[t("app.name"), t("nav.group.client")]} title={project.data.name}>
            <StatusChip status={project.data.status === "active" ? "success" : "neutral"} label={t(`project.status.${project.data.status}`)} />
          </PageTitle>
          <View style={[styles.facts, { borderColor: colors.border.subtle }]}>
            <Fact label={t("client.project_type")} value={t(`project.type.${project.data.type}`)} />
            <Fact label={t("client.project_start")} value={formatNumericDate(project.data.start_date)} numeric />
            <Fact label={t("client.project_end")} value={project.data.end_date ? formatNumericDate(project.data.end_date) : t("client.open_ended")} numeric />
          </View>
          <View style={styles.row}>
            {previewsEnabled ? (
              <>
                <Link href="/client/billing" asChild>
                  <Button variant="secondary" label={t("nav.billing")} icon="wallet" />
                </Link>
                <Link href="/client/requests" asChild>
                  <Button variant="secondary" label={t("nav.requests")} icon="mail" />
                </Link>
              </>
            ) : null}
            <Link href="/client/contact" asChild>
              <Button variant="link" label={t("nav.contact")} />
            </Link>
          </View>
        </>
      )}
    </Screen>
  );
}

function Fact({ label, value, numeric }: { label: string; value: string; numeric?: boolean }) {
  return (
    <View style={styles.fact}>
      <Txt role="meta" tone="secondary">
        {label}
      </Txt>
      <Txt role="body" numeric={numeric}>
        {value}
      </Txt>
    </View>
  );
}

/** The company's contact card (FR-012): phone, e-mail and opening hours. */
export function ContactCard({ testID }: { testID?: string }) {
  const { t, i18n } = useTranslation();
  const { api } = useSession();
  const { colors } = useTheme();
  const errorText = useErrorText();
  const contact = useLoad(() => call(api.GET("/api/v1/organisation/contact")));
  const days = weekdayNames(i18n.language, "long");
  if (contact.error) {
    return <InlineBanner tone="warning" title={errorText(contact.error)} action={{ label: t("common.retry"), onPress: () => void contact.reload() }} />;
  }
  if (contact.data === null) return <SkeletonRows rows={3} testID={testID ? `${testID}-loading` : undefined} />;
  const { phone, email, opening_hours } = contact.data;
  return (
    <View testID={testID} style={[styles.facts, { borderColor: colors.border.subtle }]}>
      <View style={styles.contactRow}>
        <Icon name="phone" size={18} color={colors.text.secondary} label={t("contact.phone")} />
        <Txt role="body" numeric testID="contact-phone">
          {phone ? isolateLtr(phone) : t("contact.not_set")}
        </Txt>
      </View>
      <View style={styles.contactRow}>
        <Icon name="mail" size={18} color={colors.text.secondary} label={t("contact.email")} />
        <Txt role="body" testID="contact-email">
          {email ? isolateLtr(email) : t("contact.not_set")}
        </Txt>
      </View>
      <View style={styles.hours}>
        <Txt role="eyebrow" tone="secondary">
          {t("contact.opening_hours")}
        </Txt>
        {opening_hours.length === 0 ? (
          <Txt role="body" tone="secondary">
            {t("contact.not_set")}
          </Txt>
        ) : (
          [...opening_hours]
            .sort((a, b) => a.weekday - b.weekday)
            .map((entry) => (
              <View key={entry.weekday} style={styles.hoursRow} testID={`contact-hours-${entry.weekday}`}>
                <Txt role="body" style={styles.hoursDay}>
                  {days[entry.weekday] ?? String(entry.weekday)}
                </Txt>
                <Txt role="body" numeric>
                  {isolateLtr(`${entry.opens}–${entry.closes}`)}
                </Txt>
              </View>
            ))
        )}
      </View>
    </View>
  );
}

/** The contact page, for clients at `/client/contact` and for staff at `/contact`. */
export function ContactScreen() {
  const { t } = useTranslation();
  return (
    <Screen testID="screen-contact">
      <PageTitle eyebrow={[t("app.name"), t("nav.contact")]} title={t("contact.title")} />
      <Txt role="body" tone="secondary">
        {t("contact.intro")}
      </Txt>
      <ContactCard testID="contact-card" />
    </Screen>
  );
}

const styles = StyleSheet.create({
  cards: { flexDirection: "row", flexWrap: "wrap", gap: 16 },
  card: { flexGrow: 1, flexBasis: 280, maxWidth: 420, gap: 8, padding: 16, borderWidth: 1 },
  cardHead: { flexDirection: "row", alignItems: "flex-start", justifyContent: "space-between", gap: 12 },
  cardTitle: { flexShrink: 1 },
  facts: { gap: 14, padding: 16, borderWidth: 1, alignSelf: "stretch", maxWidth: 640 },
  fact: { gap: 2 },
  row: { flexDirection: "row", flexWrap: "wrap", gap: 12, alignItems: "center" },
  contactRow: { flexDirection: "row", alignItems: "center", gap: 10 },
  hours: { gap: 6, marginTop: 4 },
  hoursRow: { flexDirection: "row", gap: 16, alignItems: "center" },
  hoursDay: { minWidth: 110 },
});
