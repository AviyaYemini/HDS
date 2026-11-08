/**
 * The work log and the hours report in the new design (specs/007-ux-redesign FR-050, FR-025, T088).
 *
 * Work log ("Work log" / "יומן עבודה"): who worked at one project, site or group (the API asks for
 * exactly one), opening on the first project; the hours report: hours, shifts and (where shown) pay by employee or by
 * project. Each opens on this pay period, never past today; its key figures are the API's own
 * totals; each bar opens its rows; and the rows come a page at a time.
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { InlineBanner, PageTitle, SearchablePicker, SegmentedControl, SkeletonRows, Txt } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { formatMoney, formatNumericRange } from "../../locale";
import { useLoad } from "../useLoad";
import { Bars, Figures, PagedRows, ReportRange, usePage, useReportRange, type Bar, type Figure } from "./parts";

type Scope = "project" | "site" | "group";

export function WorkLogScreen() {
  const { t } = useTranslation();
  const { api } = useSession();
  const errorText = useErrorText();
  const { range, setRange, asked, today } = useReportRange();
  const [scope, setScope] = useState<Scope>("project");
  const [picked, setPicked] = useState<string | null>(null);
  const choices = useLoad(async () => {
    const items =
      scope === "project"
        ? (await call(api.GET("/api/v1/projects"))).items.map((p) => ({ id: p.id, label: p.name }))
        : scope === "site"
          ? (await call(api.GET("/api/v1/sites"))).items.map((s) => ({ id: s.id, label: s.name }))
          : (await call(api.GET("/api/v1/groups"))).items.map((g) => ({ id: g.id, label: g.name }));
    return { scope, items };
  }, [scope]);
  // Only this scope's list counts: the previous scope's, until the new one loads, is not it.
  const listed = choices.data?.scope === scope ? choices.data.items : null;
  // The first project at first, so the log opens with figures; a site or group waits for a choice.
  const chosen = picked ?? (scope === "project" ? (listed?.[0]?.id ?? null) : null);
  const log = useLoad(async () => {
    if (chosen === null) return null;
    const query: { from: string; to: string; project_id?: string; site_id?: string; group_id?: string } = { ...asked, [`${scope}_id`]: chosen };
    return call(api.GET("/api/v1/reports/work-log", { params: { query } }));
  }, [scope, chosen, asked.from, asked.to]);

  const rows = log.data?.rows ?? [];
  const pageKey = `${scope}|${chosen}|${asked.from}|${asked.to}`;
  const figures: Figure[] = log.data
    ? [
        { key: "people", label: t("reports.figure.people"), shown: String(log.data.people), value: String(log.data.people) },
        { key: "shifts", label: t("reports.figure.shifts"), shown: String(log.data.shifts), value: String(log.data.shifts) },
        { key: "hours", label: t("reports.figure.hours"), shown: t("pay.hours", { hours: log.data.hours }), value: log.data.hours },
      ]
    : [];
  // A bar a project, sized by how many people worked there: a count of the API's rows, never a
  // sum of their hours (the figures above are the API's own totals; group G review).
  const byProject = new Map<string, { name: string; people: typeof rows }>();
  for (const row of rows) {
    const group = byProject.get(row.project_id) ?? { name: row.project_name, people: [] };
    group.people.push(row);
    byProject.set(row.project_id, group);
  }
  const bars: Bar[] = [...byProject.entries()].map(([id, g]) => ({
    id,
    name: g.name,
    hours: g.people.length,
    detail: t("reports.people_count", { count: g.people.length }),
    rows: (
      <PagedRows
        pageKey={`${pageKey}|${id}`}
        rows={g.people.map((p) => (
          <Txt key={`${p.user_id}-${p.project_id}`} role="body" testID="report-row">
            {`${p.full_name} · ${t("reports.shifts", { count: p.shifts })} · ${t("pay.hours", { hours: p.hours })} · ${formatNumericRange(p.first_date, p.last_date)}`}
          </Txt>
        ))}
      />
    ),
  }));
  const largest = Math.max(0, ...bars.map((b) => b.hours));

  return (
    <Screen testID="screen-work-log">
      <PageTitle eyebrow={[t("app.name"), t("nav.group.manage")]} title={t("nav.work_log")} />
      <ReportRange range={range} setRange={setRange} today={today} />
      <SegmentedControl<Scope>
        label={t("reports.scope")}
        options={(["project", "site", "group"] as const).map((s) => ({ value: s, label: t(`reports.kind.${s}`), testID: `scope-${s}` }))}
        value={scope}
        onChange={(next) => {
          setScope(next);
          setPicked(null);
        }}
      />
      <SearchablePicker label={t(`reports.kind.${scope}`)} items={listed ?? []} value={chosen} onChange={setPicked} testID="report-scope-picker" />
      {choices.error ? (
        <InlineBanner tone="warning" title={errorText(choices.error)} action={{ label: t("common.retry"), onPress: () => void choices.reload() }} />
      ) : log.error ? (
        <InlineBanner tone="warning" title={errorText(log.error)} action={{ label: t("common.retry"), onPress: () => void log.reload() }} />
      ) : chosen === null && listed !== null ? (
        <Txt role="body" tone="secondary">
          {t(`reports.choose.${scope}`)}
        </Txt>
      ) : log.data === null ? (
        <SkeletonRows rows={4} />
      ) : (
        <View style={styles.body}>
          <Txt role="meta" tone="secondary" numeric>
            {formatNumericRange(log.data.from, log.data.to)}
          </Txt>
          <Figures figures={figures} />
          <Bars bars={bars} title={t("reports.by_project")} largest={largest} empty={t("reports.nobody")} />
        </View>
      )}
    </Screen>
  );
}

type GroupBy = "employee" | "project";

export function HoursReportScreen() {
  const { t, i18n } = useTranslation();
  const { api } = useSession();
  const errorText = useErrorText();
  const { range, setRange, asked, today } = useReportRange();
  const [groupBy, setGroupBy] = useState<GroupBy>("employee");
  const report = useLoad(async () => call(api.GET("/api/v1/reports/hours", { params: { query: { ...asked, group_by: groupBy } } })), [asked.from, asked.to, groupBy]);
  const money = (amount: string | null | undefined) => (amount == null ? null : formatMoney(amount, i18n.language));
  const missing = (count: number) => (count > 0 ? ` · ${t("pay.missing_count", { count })}` : "");

  const data = report.data;
  const figures: Figure[] = data
    ? [
        { key: "hours", label: t("reports.figure.hours"), shown: t("pay.hours", { hours: data.hours }), value: data.hours },
        { key: "shifts", label: t("reports.figure.shifts"), shown: String(data.shifts), value: String(data.shifts) },
        ...(data.pay != null ? [{ key: "pay", label: t("reports.figure.pay"), shown: money(data.pay)!, value: data.pay }] : []),
      ]
    : [];
  const groups = data?.groups ?? [];
  const { shown, pager } = usePage(groups, `${groupBy}|${asked.from}|${asked.to}`);
  // Sized against the largest group of the whole report, so bars compare across pages.
  const largest = Math.max(0, ...groups.map((g) => Number(g.hours)));
  const bars: Bar[] = shown.map((g) => ({
    id: g.id,
    name: g.name,
    hours: Number(g.hours),
    detail: `${t("pay.hours", { hours: g.hours })}${g.pay != null ? ` · ${money(g.pay)}` : ""}${missing(g.shifts_missing_rate)}`,
    rows: g.rows.map((row) => (
      <Txt key={row.id} role="body">
        {`${row.name} · ${t("reports.shifts", { count: row.shifts })} · ${t("pay.hours", { hours: row.hours })}${row.pay != null ? ` · ${money(row.pay)}` : ""}${missing(row.shifts_missing_rate)}`}
      </Txt>
    )),
  }));

  return (
    <Screen testID="screen-hours">
      <PageTitle eyebrow={[t("app.name"), t("nav.group.manage")]} title={t("nav.hours")} />
      <ReportRange range={range} setRange={setRange} today={today} />
      <SegmentedControl<GroupBy>
        label={t("reports.group_by")}
        options={(["employee", "project"] as const).map((g) => ({ value: g, label: t(`reports.by_${g}`), testID: `group-${g}` }))}
        value={groupBy}
        onChange={setGroupBy}
      />
      {report.error ? (
        <InlineBanner tone="warning" title={errorText(report.error)} action={{ label: t("common.retry"), onPress: () => void report.reload() }} />
      ) : data === null ? (
        <SkeletonRows rows={4} />
      ) : (
        <View style={styles.body}>
          <Figures figures={figures} />
          {data.shifts_missing_rate > 0 ? <InlineBanner tone="warning" title={t("pay.missing_count", { count: data.shifts_missing_rate })} /> : null}
          <Bars bars={bars} title={t(`reports.by_${groupBy}`)} largest={largest} empty={t("reports.nothing_in_range")} />
          {pager}
        </View>
      )}
    </Screen>
  );
}

const styles = StyleSheet.create({
  body: { gap: 16 },
});
