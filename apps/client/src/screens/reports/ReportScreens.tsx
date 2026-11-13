/**
 * The work log and the hours report in the new design (specs/007-ux-redesign FR-050, FR-025, T088).
 *
 * Work log ("Work log" / "יומן עבודה"): who worked across the whole company (the default) or at one
 * project, site or group; the hours report: hours, shifts and (where shown) pay by employee or by
 * project. Each opens on this pay period, never past today; its key figures are the API's own
 * totals; each bar opens its rows; and the rows come a page at a time.
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { InlineBanner, PageTitle, SearchablePicker, SegmentedControl, SkeletonRows, Table, Txt, type Column } from "../../design/components";
import { useTheme } from "../../design/theme/ThemeProvider";
import { CardList, DashboardSkeleton, KeyFigures, type KeyFigure } from "../deck/parts";
import { useEntrance } from "../deck/useEntrance";
import { formatHoursFigure } from "./HoursCostsScreen";
import { Screen } from "../../design/components/Screen";
import { formatMoney, formatNumericRange, presetRange } from "../../locale";
import { useLoad } from "../useLoad";
import { Bars, Figures, ReportRange, usePage, useReportRange, type Bar, type Figure } from "./parts";

/** Who the log covers: the whole company (the default), or one project, site or group. */
type Scope = "company" | "project" | "site" | "group";

/** The work log's periods (the owner, 2026-11-13): compact chips, this month first; custom opens the dates. */
const WL_PRESETS = ["this-month", "last-month", "last-30-days", "custom"] as const;
type WlPreset = (typeof WL_PRESETS)[number];

export function WorkLogScreen() {
  const { t, i18n } = useTranslation();
  const { api } = useSession();
  const errorText = useErrorText();
  const { isDesktop } = useTheme();
  // This month by default, with results showing (it was a full-page calendar).
  const { range, setRange, asked, today } = useReportRange("this-month");
  const [preset, setPreset] = useState<WlPreset>("this-month");
  const choose = (next: WlPreset) => {
    setPreset(next);
    if (next !== "custom") setRange(presetRange(next, today));
  };
  const [scope, setScope] = useState<Scope>("company");
  const [picked, setPicked] = useState<string | null>(null);
  const choices = useLoad(async () => {
    if (scope === "company") return { scope, items: [] };
    const items =
      scope === "project"
        ? (await call(api.GET("/api/v1/projects"))).items.map((p) => ({ id: p.id, label: p.name }))
        : scope === "site"
          ? (await call(api.GET("/api/v1/sites"))).items.map((x) => ({ id: x.id, label: x.name }))
          : (await call(api.GET("/api/v1/groups"))).items.map((g) => ({ id: g.id, label: g.name }));
    return { scope, items };
  }, [scope]);
  // Only this scope's list counts: the previous scope's, until the new one loads, is not it.
  const listed = choices.data?.scope === scope ? choices.data.items : null;
  // The whole company needs no choice; a project, site or group waits for one.
  const chosen = scope === "company" ? "company" : picked;
  const log = useLoad(async () => {
    if (chosen === null) return null;
    const query: { from: string; to: string; project_id?: string; site_id?: string; group_id?: string } =
      scope === "company" ? { ...asked } : { ...asked, [`${scope}_id`]: chosen };
    return call(api.GET("/api/v1/reports/work-log", { params: { query } }));
  }, [scope, chosen, asked.from, asked.to]);
  const progress = useEntrance(log.data ? `${log.data.from}|${log.data.to}|${scope}|${chosen}` : null);

  type Row = NonNullable<typeof log.data>["rows"][number];
  const rows: Row[] = [...(log.data?.rows ?? [])].sort((a, b) => Number(b.hours) - Number(a.hours));
  const pageKey = `${scope}|${chosen}|${asked.from}|${asked.to}`;
  const { shown, pager } = usePage(rows, pageKey);
  const hours = (value: string) => formatHoursFigure(value, i18n.language);
  // The figures are the API's own totals, never the rows added up.
  const figures: KeyFigure[] = log.data
    ? [
        { key: "people", label: t("reports.figure.people"), value: String(log.data.people), count: { to: log.data.people, format: (n: number) => String(Math.round(n)) } },
        // Each person on each shift once, unlike the hours report's distinct shifts (CodeRabbit, PR 20).
        { key: "shifts", label: t("reports.figure.person_shifts"), value: String(log.data.shifts), count: { to: log.data.shifts, format: (n: number) => String(Math.round(n)) } },
        { key: "hours", label: t("reports.figure.hours"), value: hours(log.data.hours), raw: log.data.hours, count: { to: Number(log.data.hours), format: (n: number) => hours(n.toFixed(2)) } },
      ]
    : [];
  const columns: Column<Row>[] = [
    { key: "name", title: t("wl.col.person"), render: (r) => <Txt role="label">{r.full_name}</Txt> },
    { key: "project", title: t("wl.col.project"), render: (r) => <Txt role="body">{r.project_name}</Txt> },
    { key: "shifts", title: t("wl.col.shifts"), numeric: true, width: 90 },
    { key: "hours", title: t("wl.col.hours"), numeric: true, width: 90, render: (r) => <Txt role="body" numeric>{hours(r.hours)}</Txt> },
    { key: "dates", title: t("wl.col.dates"), width: 220, render: (r) => <Txt role="meta" tone="secondary" numeric>{formatNumericRange(r.first_date, r.last_date)}</Txt> },
  ];

  return (
    <Screen testID="screen-work-log">
      <PageTitle eyebrow={[t("app.name"), t("nav.group.manage")]} title={t("nav.work_log")} />
      <View style={styles.controls}>
        <SegmentedControl<WlPreset>
          label={t("reports.range_label")}
          options={WL_PRESETS.map((p) => ({ value: p, label: t(`wl.preset.${p}`), testID: `preset-${p}` }))}
          value={preset}
          onChange={choose}
        />
        <SegmentedControl<Scope>
          label={t("reports.scope")}
          options={(["company", "project", "site", "group"] as const).map((x) => ({ value: x, label: t(`reports.kind.${x}`), testID: `scope-${x}` }))}
          value={scope}
          onChange={(next) => {
            setScope(next);
            setPicked(null);
          }}
        />
      </View>
      {preset === "custom" ? <ReportRange range={range} setRange={setRange} today={today} /> : null}
      {scope !== "company" ? (
        <SearchablePicker label={t(`reports.kind.${scope}`)} items={listed ?? []} value={picked} onChange={setPicked} testID="report-scope-picker" />
      ) : null}
      {choices.error ? (
        <InlineBanner tone="warning" title={errorText(choices.error)} action={{ label: t("common.retry"), onPress: () => void choices.reload() }} />
      ) : log.error ? (
        <InlineBanner tone="warning" title={errorText(log.error)} action={{ label: t("common.retry"), onPress: () => void log.reload() }} />
      ) : chosen === null && listed !== null ? (
        <Txt role="body" tone="secondary">
          {t(`reports.choose.${scope}`)}
        </Txt>
      ) : log.data === null ? (
        <DashboardSkeleton figures={3} charts={0} rows={8} />
      ) : (
        <View style={styles.body}>
          <Txt role="meta" tone="secondary" numeric>
            {formatNumericRange(log.data.from, log.data.to)}
          </Txt>
          <KeyFigures figures={figures} progress={progress} />
          {rows.length === 0 ? (
            <Txt role="body" tone="secondary">
              {t("reports.nobody")}
            </Txt>
          ) : isDesktop ? (
            <Table columns={columns} rows={shown} keyOf={(r) => `${r.user_id}-${r.project_id}`} caption={t("nav.work_log")} testID="wl-table" />
          ) : (
            <CardList
              testID="wl-table"
              items={shown.map((r) => ({
                key: `${r.user_id}-${r.project_id}`,
                title: r.full_name,
                subtitle: r.project_name,
                figures: [
                  { label: t("wl.col.shifts"), value: String(r.shifts) },
                  { label: t("wl.col.hours"), value: hours(r.hours) },
                  { label: t("wl.col.dates"), value: formatNumericRange(r.first_date, r.last_date) },
                ],
              }))}
            />
          )}
          {pager}
        </View>
      )}
    </Screen>
  );
}

type GroupBy = "employee" | "project" | "client";

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
        options={(["employee", "project", "client"] as const).map((g) => ({ value: g, label: t(`reports.by_${g}`), testID: `group-${g}` }))}
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
  controls: { flexDirection: "row", flexWrap: "wrap", gap: 16, alignItems: "flex-end" },
});
