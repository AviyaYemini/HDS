/**
 * Home per role (specs/007-ux-redesign FR-006, N7). An employee lands on their next shift and the
 * availability deadline; a manager on today's coverage, unfilled seats and who hasn't submitted,
 * which stay placeholders until groups C and D serve that data (never invented numbers); an admin
 * also sees the way into the setup checklist. The chooser asks for capabilities, not roles.
 */
import { Link, useRouter } from "expo-router";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { useCapabilities } from "../../capabilities";
import {
  Button,
  EmptyState,
  FeaturePanel,
  InlineBanner,
  LinkPressable,
  PageTitle,
  Skeleton,
  StatusChip,
  Txt,
} from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { Icon, type IconName } from "../../design/icons/Icon";
import { useTheme } from "../../design/theme/ThemeProvider";
import {
  DEFAULT_TIME_ZONE,
  addDays,
  dateOf,
  daysBetween,
  formatHeaderDate,
  formatNumericDate,
  formatNumericRange,
  formatTimeRange,
  todayIn,
} from "../../locale";
import { useLoad } from "../useLoad";

/** The next published shift, as a feature panel (FR-022's first form; group C finishes it). */
export function NextShiftPanel() {
  const router = useRouter();
  const { t, i18n } = useTranslation();
  const { colors } = useTheme();
  const { api } = useSession();
  const today = todayIn(DEFAULT_TIME_ZONE);
  const shifts = useLoad(() => call(api.GET("/me/shifts", { params: { query: { from: today } } })), [today]);
  const next = shifts.data?.items[0] ?? null;

  if (shifts.error) {
    return <InlineBanner tone="warning" title={t("home.next_shift_unavailable")} testID="home-next-shift-error" />;
  }
  if (shifts.data === null) {
    return (
      <FeaturePanel testID="home-next-shift-loading">
        <Skeleton width={96} height={12} />
        <Skeleton width="60%" height={22} />
        <Skeleton width="40%" height={14} />
      </FeaturePanel>
    );
  }
  if (next === null) {
    return (
      <EmptyState
        testID="home-next-shift-none"
        icon="calendar"
        title={t("home.no_upcoming_title")}
        body={t("home.no_upcoming_body")}
        action={{ label: t("nav.availability"), onPress: () => router.push("/my/availability"), testID: "home-open-availability" }}
      />
    );
  }
  const date = dateOf(next.starts_at, next.timezone);
  const inDays = daysBetween(today, date);
  const when = inDays === 0 ? t("home.today") : inDays === 1 ? t("home.tomorrow") : t("home.in_days", { count: inDays });
  return (
    <FeaturePanel testID="home-next-shift">
      <Txt role="eyebrow" tone="secondary">
        {t("home.next_shift")}
      </Txt>
      <Txt role="section" heading={2}>
        {`${next.project.name} · ${next.shift_type}`}
      </Txt>
      <Txt role="body" numeric>
        {`${formatHeaderDate(date, i18n.language)}, ${formatTimeRange(next.starts_at, next.ends_at, next.timezone)}`}
      </Txt>
      <View style={styles.row}>
        <Icon name="pin" size={16} color={colors.inverse.textSecondary} />
        <Txt role="meta" tone="secondary">
          {next.site.name}
        </Txt>
        <StatusChip status="ready" label={when} />
      </View>
    </FeaturePanel>
  );
}

/** The open window's deadline, when there is one (FR-006, E2). */
export function AvailabilityDeadline() {
  const router = useRouter();
  const { t } = useTranslation();
  const { api } = useSession();
  const windows = useLoad(() => call(api.GET("/api/v1/availability-windows")));
  const open = windows.data?.items.find((window) => window.is_open) ?? null;
  if (!open) return null;
  const deadline = dateOf(open.deadline, DEFAULT_TIME_ZONE);
  return (
    <InlineBanner
      tone="info"
      title={t("home.deadline_title", { date: formatNumericDate(deadline) })}
      action={{ label: t("home.open_availability"), onPress: () => router.push("/my/availability"), testID: "home-deadline-open" }}
      testID="home-deadline"
    >
      {t("home.deadline_body", { range: formatNumericRange(open.starts_on, open.ends_on) })}
    </InlineBanner>
  );
}

function QuickLink({ href, icon, label, testID }: { href: string; icon: IconName; label: string; testID: string }) {
  return (
    <Link href={href as never} asChild>
      <Button variant="secondary" label={label} icon={icon} testID={testID} />
    </Link>
  );
}

function PendingCard({ titleKey, testID }: { titleKey: string; testID: string }) {
  const { t } = useTranslation();
  const { colors } = useTheme();
  return (
    <View testID={testID} style={[styles.card, { borderColor: colors.border.subtle, backgroundColor: colors.bg.surface }]}>
      <Txt role="eyebrow" tone="secondary">
        {t(titleKey)}
      </Txt>
      <Skeleton width="50%" height={28} />
      <Skeleton width="80%" height={12} />
      <Txt role="meta" tone="secondary">
        {t("home.card_pending")}
      </Txt>
    </View>
  );
}

function firstName(fullName: string | undefined): string {
  return (fullName ?? "").trim().split(/\s+/)[0] ?? "";
}

function EmployeeHome() {
  const { t } = useTranslation();
  const session = useSession();
  return (
    <Screen testID="screen-home">
      <PageTitle eyebrow={[t("app.name"), t("nav.group.my_work")]} title={t("home.greeting", { name: firstName(session.user?.full_name) })} />
      <AvailabilityDeadline />
      <NextShiftPanel />
      <View style={styles.row}>
        <QuickLink href="/my/availability" icon="calendar" label={t("nav.availability")} testID="home-link-availability" />
        <QuickLink href="/my/shifts" icon="clock" label={t("nav.shifts")} testID="home-link-shifts" />
        <QuickLink href="/my/pay" icon="wallet" label={t("nav.pay")} testID="home-link-pay" />
      </View>
    </Screen>
  );
}

/**
 * Today's coverage, the open seats on today's shifts, and who has not yet submitted for the nearest
 * open availability window (FR-006, T074), each linking to where it is dealt with.
 */
function ManagerCards() {
  const { t } = useTranslation();
  const session = useSession();
  const today = todayIn(DEFAULT_TIME_ZONE);
  const shifts = useLoad(async () => (await call(session.api.GET("/api/v1/shifts", { params: { query: { from: today, to: today, limit: 200 } } }))).items, [today]);
  const missing = useLoad(async () => {
    const windows = (await call(session.api.GET("/api/v1/availability-windows", { params: { query: { from: today, to: addDays(today, 60) } } }))).items;
    const open = windows.filter((w) => new Date(w.deadline).getTime() > Date.now()).sort((a, b) => a.deadline.localeCompare(b.deadline))[0];
    if (!open) return { window: null, count: 0 };
    const rows = (await call(session.api.GET("/api/v1/availability-windows/{id}/submissions", { params: { path: { id: open.id } } }))).items;
    return { window: open.id, count: rows.filter((r) => r.status === "not_submitted").length };
  }, [today]);
  const filled = (shifts.data ?? []).reduce((n, s) => n + s.assignments.length, 0);
  const required = (shifts.data ?? []).reduce((n, s) => n + s.headcount, 0);
  const open = (shifts.data ?? []).reduce((n, s) => n + Math.max(0, s.headcount - s.assignments.length), 0);
  return (
    <View style={styles.cards}>
      {shifts.data === null ? (
        <>
          <PendingCard titleKey="home.coverage_today" testID="home-card-coverage" />
          <PendingCard titleKey="home.unfilled" testID="home-card-unfilled" />
        </>
      ) : (
        <>
          <FigureCard titleKey="home.coverage_today" value={`${filled}/${required}`} href="/roster" testID="home-card-coverage" />
          <FigureCard titleKey="home.unfilled" value={String(open)} href="/roster" testID="home-card-unfilled" />
        </>
      )}
      {missing.data === null && !missing.error ? (
        <PendingCard titleKey="home.not_submitted" testID="home-card-not-submitted" />
      ) : missing.data?.window ? (
        <FigureCard titleKey="home.not_submitted" value={String(missing.data.count)} href={`/roster/submissions?window=${missing.data.window}`} testID="home-card-not-submitted" />
      ) : (
        <FigureCard titleKey="home.not_submitted" value={t("home.no_open_window")} href="/roster/submissions" testID="home-card-not-submitted" />
      )}
    </View>
  );
}

function FigureCard({ titleKey, value, href, testID }: { titleKey: string; value: string; href: string; testID: string }) {
  const { t } = useTranslation();
  const { colors } = useTheme();
  return (
      <LinkPressable href={href} testID={testID} role="link" style={() => [styles.card, { borderColor: colors.border.subtle, backgroundColor: colors.bg.surface }]}>
        <Txt role="eyebrow" tone="secondary">
          {t(titleKey)}
        </Txt>
        <Txt role="display" numeric>
          {value}
        </Txt>
      </LinkPressable>
  );
}

function ManagerHome({ admin }: { admin: boolean }) {
  const { t } = useTranslation();
  const session = useSession();
  const { colors } = useTheme();
  return (
    <Screen testID="screen-home">
      <PageTitle eyebrow={[t("app.name"), t("nav.group.manage")]} title={t("home.greeting", { name: firstName(session.user?.full_name) })} />
      <ManagerCards />
      <View style={styles.row}>
        <QuickLink href="/roster" icon="grid" label={t("nav.roster")} testID="home-link-roster" />
        <QuickLink href="/roster/submissions" icon="list" label={t("nav.submissions")} testID="home-link-submissions" />
        <QuickLink href="/runs" icon="play" label={t("nav.runs")} testID="home-link-runs" />
      </View>
      {admin ? (
        <View testID="home-setup" style={[styles.card, { borderColor: colors.border.strong }]}>
          <Txt role="eyebrow" tone="secondary">
            {t("nav.group.admin")}
          </Txt>
          <Txt role="section" heading={2}>
            {t("home.setup_title")}
          </Txt>
          <Txt role="body" tone="secondary">
            {t("home.setup_body")}
          </Txt>
          <View style={styles.row}>
            <QuickLink href="/admin/sites" icon="building" label={t("nav.sites")} testID="home-setup-sites" />
            <QuickLink href="/admin/projects" icon="layers" label={t("nav.projects")} testID="home-setup-projects" />
            <QuickLink href="/admin/shift-templates" icon="clock" label={t("nav.shift_templates")} testID="home-setup-templates" />
            <QuickLink href="/admin/staff" icon="people" label={t("nav.staff")} testID="home-setup-staff" />
          </View>
        </View>
      ) : null}
      <AvailabilityDeadline />
      <NextShiftPanel />
    </Screen>
  );
}

/** The home for whoever is signed in. */
export function HomeScreen() {
  const capabilities = useCapabilities();
  if (capabilities.has("roster.read")) return <ManagerHome admin={capabilities.has("special_days.edit")} />;
  return <EmployeeHome />;
}

const styles = StyleSheet.create({
  row: { flexDirection: "row", flexWrap: "wrap", gap: 12, alignItems: "center" },
  cards: { flexDirection: "row", flexWrap: "wrap", gap: 16 },
  card: { flexGrow: 1, flexBasis: 220, gap: 10, padding: 16, borderWidth: 1 },
});
