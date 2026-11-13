/**
 * The snapshot dashboard (specs/demo-ceo/plan.md, screen 1; deck slide 14), where an Admin or a
 * Manager lands: a month, quarter or year switch; four numbered key figures (billing so far, the
 * forecast to the period's end, this week's coverage, people); a dark panel for temporary workers;
 * weekly bars of billing against pay with the forecast weeks lighter; and the project table with
 * coverage, margin and a status chip. Every amount is the API's own; pay shows only where sent.
 */
import { useRouter } from "expo-router";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { FeaturePanel, InlineBanner, LinkPressable, PageTitle, SegmentedControl, SkeletonRows, StatusChip, Table, Txt, type Column } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { isRtl } from "../../i18n";
import { DEFAULT_TIME_ZONE, formatHeaderDate, formatMoney, isolateLtr, todayIn } from "../../locale";
import { formatHoursFigure } from "../reports/HoursCostsScreen";
import { Candles, Donut } from "../deck/Charts";
import { pieSlices, sumAmounts, weeklyCandles } from "../deck/chartMath";
import { CardList, ColumnBars, KeyFigures, SectionHead, Sparkline, Split, type BarColumn, type KeyFigure } from "../deck/parts";
import { changePct, changePoints, firstName, formatChange, formatMoneyWhole, greetingKey } from "../deck/polish";
import { useEntrance } from "../deck/useEntrance";
import { rosterHref } from "../roster/ProjectChooser";
import { useLoad } from "../useLoad";
import { coveragePercent, formatPercent, healthChip, periodRange, weekLabel, type OverviewProject, type Period } from "./model";

/** The temporary slice's key: the one the accent highlights. */
const TEMPORARY = "temporary";

export function OverviewScreen() {
  const { t, i18n } = useTranslation();
  const session = useSession();
  const { api } = session;
  const router = useRouter();
  const errorText = useErrorText();
  const { colors, isDesktop } = useTheme();
  const today = todayIn(DEFAULT_TIME_ZONE);
  const [period, setPeriod] = useState<Period>("month");
  const range = periodRange(period, today);
  const overview = useLoad(async () => call(api.GET("/api/v1/reports/overview", { params: { query: range } })), [range.from, range.to]);
  const money = (amount: string) => formatMoney(amount, i18n.language);
  // On the dashboard, whole shekels; the detailed tables keep their agorot (the owner, 2026-11-13).
  const whole = (amount: string) => formatMoneyWhole(amount, i18n.language);
  const countMoney = (n: number) => formatMoneyWhole(Math.round(n).toFixed(2), i18n.language);
  const d = overview.data;
  // Numbers count up, bars, candles and donuts grow, once per answer (reduced motion: at once).
  const progress = useEntrance(d ? `${d.from}|${d.to}` : null);
  const prev = d?.previous ?? null;
  const vsPrevious = (pct: number | null) => {
    const shown = formatChange(pct);
    // Isolated left to right, so the arrow stays before its number inside Hebrew.
    return shown ? t(`snapshot.change.${period}`, { change: isolateLtr(shown) }) : null;
  };
  // The forecast's comparison: the same days of the period before, up to where the roster runs.
  const previousName =
    prev && period === "month" ? new Intl.DateTimeFormat(i18n.language === "he" ? "he-IL" : "en-GB", { month: "long", timeZone: "UTC" }).format(new Date(`${prev.from}T12:00:00Z`)) : "";
  const vsSameSpan = (pct: number | null) => {
    const shown = formatChange(pct);
    return shown ? t(`snapshot.change_span.${period}`, { change: isolateLtr(shown), month: previousName }) : null;
  };
  const rosteredTo = d?.rostered_until ? t("snapshot.rostered_until", { date: weekLabel(d.rostered_until) }) : null;
  const hour = Number(new Intl.DateTimeFormat("en-GB", { hour: "numeric", hourCycle: "h23", timeZone: DEFAULT_TIME_ZONE }).format(new Date()));
  const greeting = `${t(greetingKey(hour), { name: firstName(session.user?.full_name) })} · ${formatHeaderDate(today, i18n.language)}`;

  const title =
    period === "month"
      ? new Intl.DateTimeFormat(i18n.language === "he" ? "he-IL" : "en-GB", { month: "long", year: "numeric", timeZone: "UTC" }).format(new Date(`${range.from}T12:00:00Z`))
      : period === "quarter"
        ? t("snapshot.quarter_title", { quarter: Math.floor((Number(range.from.slice(5, 7)) - 1) / 3) + 1, year: range.from.slice(0, 4) })
        : range.from.slice(0, 4);

  const coverage = d ? coveragePercent(d.coverage) : null;
  const coverageNext = d ? coveragePercent(d.coverage_next) : null;
  const figures: KeyFigure[] = d
    ? [
        // Where pay is hidden the API sends no money at all: the first two figures are hours then.
        ...(d.billing_actual != null && d.billing_forecast != null
          ? [
              {
                key: "billing",
                label: t("snapshot.figure.billing"),
                value: whole(d.billing_actual),
                raw: d.billing_actual,
                count: { to: Number(d.billing_actual), format: countMoney },
                // Against the same days of the previous period, so "so far" compares fairly.
                change: vsPrevious(changePct(d.billing_actual, prev?.billing_to_date)),
                detail: d.pay_actual != null ? t("snapshot.figure.billing_detail", { pay: whole(d.pay_actual), hours: formatHoursFigure(d.hours_actual, i18n.language) }) : t("snapshot.figure.billing_hours", { hours: formatHoursFigure(d.hours_actual, i18n.language) }),
              },
              {
                key: "forecast",
                label: t(`snapshot.figure.forecast.${period}`),
                value: whole(d.billing_forecast),
                raw: d.billing_forecast,
                count: { to: Number(d.billing_forecast), format: countMoney },
                // Against the same span of the period before: the forecast runs only as far as the roster.
                change: vsSameSpan(changePct(d.billing_forecast, prev?.billing_to_rostered)),
                // Where the roster (and so the forecast) stops, and the margin with its change.
                detail: (() => {
                  const points = changePoints(d.margin_pct, prev?.margin_pct);
                  const margin = d.margin_pct != null ? t("snapshot.margin_is", { margin: formatPercent(d.margin_pct) }) : null;
                  const change = points === null || margin === null ? null : t("snapshot.change.margin_short", { change: isolateLtr(formatChange(points)!.replace("%", "")) });
                  return [rosteredTo ?? t("snapshot.figure.forecast_detail"), [margin, change].filter(Boolean).join(" ")].filter(Boolean).join(" · ");
                })(),
                tone: (d.margin_pct != null && Number(d.margin_pct) < 0 ? "danger" : "primary") as KeyFigure["tone"],
              },
            ]
          : [
              {
                key: "hours",
                label: t("snapshot.figure.hours"),
                value: formatHoursFigure(d.hours_actual, i18n.language),
                raw: d.hours_actual,
                count: { to: Number(d.hours_actual), format: (n: number) => formatHoursFigure(n.toFixed(2), i18n.language) },
                change: vsPrevious(changePct(d.hours_actual, prev?.hours_to_date)),
                detail: t("snapshot.figure.hours_detail"),
              },
              {
                key: "forecast",
                label: t(`snapshot.figure.forecast_hours.${period}`),
                value: formatHoursFigure(d.hours_forecast, i18n.language),
                raw: d.hours_forecast,
                count: { to: Number(d.hours_forecast), format: (n: number) => formatHoursFigure(n.toFixed(2), i18n.language) },
                change: vsSameSpan(changePct(d.hours_forecast, prev?.hours_to_rostered)),
                detail: rosteredTo ?? t("snapshot.figure.forecast_detail"),
              },
            ]),
        {
          key: "coverage",
          label: t("snapshot.figure.coverage"),
          value: coverage === null ? "—" : `${coverage}%`,
          raw: coverage === null ? "" : String(coverage),
          ...(coverage === null ? {} : { count: { to: coverage, format: (n: number) => `${Math.round(n)}%` } }),
          detail: d.coverage.empty_places === 0 ? t("snapshot.figure.coverage_full") : t("snapshot.figure.coverage_detail", { count: d.coverage.empty_places, projects: d.coverage.projects_with_gaps }),
          // Next week, where the open places are (the drafts not yet filled).
          change: coverageNext === null ? null : t("snapshot.figure.coverage_next", { pct: coverageNext, count: d.coverage_next.empty_places }),
        },
        {
          key: "people",
          label: t("snapshot.figure.people"),
          value: String(d.people.working_this_week),
          count: { to: d.people.working_this_week, format: (n: number) => String(Math.round(n)) },
          unit: t("snapshot.figure.people_unit", { active: d.people.active }),
          raw: String(d.people.working_this_week),
          detail: t("snapshot.figure.people_detail"),
        },
      ]
    : [];

  // Billing against pay by week; hours alone where the money is hidden.
  const showBilling = (d?.weeks ?? []).some((w) => w.billing != null);
  const showPay = (d?.weeks ?? []).some((w) => w.pay != null);
  const bars: BarColumn[] = (d?.weeks ?? []).map((w) => ({
    key: w.week_start,
    label: w.forecast ? `${weekLabel(w.week_start)} · ${t("snapshot.forecast_short")}` : weekLabel(w.week_start),
    dark: showBilling ? Number(w.billing ?? 0) : Number(w.hours),
    light: w.pay != null ? Number(w.pay) : null,
    faded: w.forecast,
    title: `${weekLabel(w.week_start)}: ${w.billing != null ? t("snapshot.bar_title", { billing: money(w.billing) }) : t("pay.hours", { hours: w.hours })}${w.pay != null ? ` · ${t("snapshot.bar_pay", { pay: money(w.pay) })}` : ""}`,
  }));

  // Candles: a week of daily billing each; none where the money is hidden.
  const candles = weeklyCandles(d?.days ?? []);
  const tickText = (v: number) => (Math.abs(v) >= 1000 ? `₪${(v / 1000).toFixed(v % 1000 === 0 ? 0 : 1)}K` : `₪${v}`);
  const candlesSummary =
    candles.length > 0
      ? t("snapshot.chart.candles_summary", {
          count: candles.length,
          low: money(Math.min(...candles.map((c) => c.low)).toFixed(2)),
          high: money(Math.max(...candles.map((c) => c.high)).toFixed(2)),
          rising: candles.filter((c) => c.rising).length,
        })
      : "";
  // Billing by client, the five largest and the rest; only where money is shown.
  const clientSlices = d && d.projects.every((p) => p.billing != null) ? pieSlices(d.projects.map((p) => ({ label: p.client_name, amount: p.billing! })), 5, t("snapshot.chart.others")) : [];
  const clientsSummary =
    clientSlices.length > 0
      ? t("snapshot.chart.clients_summary", { client: clientSlices[0]!.label, pct: Math.round(clientSlices[0]!.pct), total: money(sumAmounts(clientSlices.map((sl) => sl.amount))), count: clientSlices.length })
      : "";
  // Permanent against temporary, from the people the API counts.
  const permanent = d ? Math.max(0, d.people.active - d.people.temporary) : 0;
  const peopleSlices = d && d.people.active > 0 ? pieSlices([{ label: t("staff.type.permanent"), amount: String(permanent) }, { label: t("staff.type.temporary"), amount: String(d.people.temporary) }], 2, "").map((sl) => ({ ...sl, key: sl.label === t("staff.type.temporary") ? TEMPORARY : "permanent" })) : [];
  const peopleSummary = d ? t("snapshot.chart.people_summary", { permanent, temporary: d.people.temporary, pct: d.people.active > 0 ? Math.round((d.people.temporary / d.people.active) * 100) : 0 }) : "";

  /** A project's row opens its roster week (this week). */
  const openProject = (id: string) => router.push(rosterHref(id) as never);

  const columns: Column<OverviewProject>[] = [
    {
      key: "name",
      title: t("snapshot.col.project"),
      render: (p) => (
        <View style={styles.nameCell} {...webProps({ title: p.name })}>
          {/* One line, the estimate mark right after the name; the full name on hover. */}
          <Txt role="label" numberOfLines={1}>
            {p.terms_estimated ? `${p.name} *` : p.name}
          </Txt>
          <Txt role="meta" tone="secondary" numberOfLines={1}>
            {p.client_name}
            {p.temporary_workers > 0 ? ` · ${t("snapshot.temp_on_project", { count: p.temporary_workers })}` : ""}
          </Txt>
        </View>
      ),
    },
    { key: "hours", title: t("snapshot.col.hours"), numeric: true, width: 90, render: (p) => <Txt role="body" numeric>{formatHoursFigure(p.hours, i18n.language)}</Txt> },
    ...(d?.projects.some((p) => p.billing != null)
      ? [{ key: "billing", title: t("snapshot.col.billing"), numeric: true, width: 140, render: (p: OverviewProject) => <Txt role="body" numeric numberOfLines={1}>{p.billing != null ? money(p.billing) : "—"}</Txt> }]
      : []),
    {
      key: "trend",
      title: t("snapshot.col.trend"),
      width: 96,
      render: (p) => <Sparkline values={p.weeks.map((w) => Number(w.hours))} rtl={isRtl()} testID={`trend-${p.id}`} />,
    },
    {
      key: "coverage",
      title: t("snapshot.col.coverage"),
      numeric: true,
      width: 110,
      render: (p) => (
        <View style={styles.coverageCell}>
          <Txt role="body" numeric>
            {formatPercent(p.coverage_pct)}
          </Txt>
          {p.empty_places_next > 0 ? (
            <Txt role="meta" tone="secondary" testID={`next-${p.id}`}>
              {t("snapshot.next_empty", { count: p.empty_places_next })}
            </Txt>
          ) : null}
        </View>
      ),
    },
    // Margin needs pay: where the API hides pay there is no margin column at all, never a dash.
    ...(d?.projects.some((p) => p.margin_pct != null)
      ? [{ key: "margin", title: t("snapshot.col.margin"), numeric: true, width: 90, render: (p: OverviewProject) => <Txt role="body" numeric tone={p.margin_pct != null && Number(p.margin_pct) < 0 ? "danger" : "primary"}>{formatPercent(p.margin_pct)}</Txt> }]
      : []),
    {
      key: "status",
      title: t("snapshot.col.status"),
      width: 130,
      render: (p) => <StatusChip status={healthChip(p.status)} label={p.status === "gaps" ? t("snapshot.status.gaps", { count: p.empty_places }) : t(`snapshot.status.${p.status}`)} testID={`status-${p.id}`} />,
    },
  ];

  return (
    <Screen testID="screen-overview">
      <Txt role="body" tone="secondary" testID="greeting">
        {greeting}
      </Txt>
      <PageTitle eyebrow={[t("app.name"), t("nav.overview")]} title={title}>
        <SegmentedControl<Period>
          label={t("snapshot.period")}
          options={(["month", "quarter", "year"] as const).map((p) => ({ value: p, label: t(`snapshot.period_${p}`), testID: `period-${p}` }))}
          value={period}
          onChange={setPeriod}
        />
      </PageTitle>
      {overview.error ? (
        <InlineBanner tone="warning" title={errorText(overview.error)} action={{ label: t("common.retry"), onPress: () => void overview.reload() }} />
      ) : d === null ? (
        <SkeletonRows rows={6} />
      ) : (
        <View style={styles.body}>
          <KeyFigures figures={figures} progress={progress} />
          <Split>
            <View style={styles.chart}>
              <SectionHead title={showPay ? t("snapshot.bars_title") : showBilling ? t("snapshot.bars_title_billing") : t("snapshot.bars_title_hours")} />
              <ColumnBars columns={bars} legend={showPay ? t("snapshot.legend") : t("snapshot.legend_billing")} progress={progress} />
            </View>
            {candles.length > 0 ? (
              <View style={styles.chartWide}>
                <SectionHead title={t("snapshot.chart.candles_title")} />
                <Candles candles={candles} labelOf={(c) => weekLabel(c.week)} tickText={tickText} summary={candlesSummary} testID="chart-candles" progress={progress} />
                <Txt role="meta" tone="secondary">
                  {t("snapshot.chart.candles_legend")}
                </Txt>
              </View>
            ) : null}
          </Split>
          <Split>
            {clientSlices.length > 0 ? (
              <View style={styles.chart}>
                <SectionHead title={t("snapshot.chart.clients_title")} />
                <Donut slices={clientSlices} amountText={(sl) => whole(sl.amount)} summary={clientsSummary} testID="chart-clients" progress={progress} />
              </View>
            ) : null}
            {peopleSlices.length > 0 ? (
              <View style={styles.chart}>
                <SectionHead title={t("snapshot.chart.people_title")} />
                <Donut
                  slices={peopleSlices}
                  colorOf={(sl) => (sl.key === TEMPORARY ? colors.accent.default : colors.text.primary)}
                  amountText={(sl) => String(Math.round(sl.value))}
                  centre={String(d.people.active)}
                  summary={peopleSummary}
                  testID="chart-people"
                  progress={progress}
                />
              </View>
            ) : null}
            {d.people.temporary > 0 ? (
              <View style={styles.chart}>
                <FeaturePanel testID="panel-temporary">
                  <Txt role="eyebrow" tone="accent">
                    {t("snapshot.temp_eyebrow")}
                  </Txt>
                  <Txt role="display" numeric>
                    {String(d.people.temporary)}
                  </Txt>
                  <Txt role="meta" tone="secondary">
                    {t("snapshot.temp_ending", { count: d.people.temporary_ending_soon })}
                  </Txt>
                  <LinkPressable href="/admin/staff?type=temporary" testID="link-temporary">
                    <Txt role="label" tone="accent">
                      {t("snapshot.temp_open")}
                    </Txt>
                  </LinkPressable>
                </FeaturePanel>
              </View>
            ) : null}
          </Split>
          {/* The project table at full width: Hebrew names stay on one line (the helper's review). */}
          <View style={styles.table}>
            <SectionHead title={t("snapshot.projects_title")} note={t("snapshot.projects_note", { count: d.projects.length })} />
            {isDesktop ? (
              <Table columns={columns} rows={d.projects} keyOf={(p) => p.id} caption={t("snapshot.projects_title")} testID="overview-projects" onRowPress={(p) => openProject(p.id)} />
            ) : (
              // A phone: a card a project, the name on top, its key numbers and its status (the owner, 2026-11-13).
              <CardList
                testID="overview-projects"
                items={d.projects.map((p) => ({
                  key: p.id,
                  title: p.terms_estimated ? `${p.name} *` : p.name,
                  subtitle: `${p.client_name}${p.temporary_workers > 0 ? ` · ${t("snapshot.temp_on_project", { count: p.temporary_workers })}` : ""}`,
                  figures: [
                    ...(p.billing != null ? [{ label: t("snapshot.col.billing"), value: money(p.billing) }] : [{ label: t("snapshot.col.hours"), value: formatHoursFigure(p.hours, i18n.language) }]),
                    { label: t("snapshot.col.coverage"), value: p.empty_places_next > 0 ? `${formatPercent(p.coverage_pct)} · ${t("snapshot.next_empty", { count: p.empty_places_next })}` : formatPercent(p.coverage_pct) },
                    ...(p.margin_pct != null ? [{ label: t("snapshot.col.margin"), value: formatPercent(p.margin_pct), tone: (Number(p.margin_pct) < 0 ? "danger" : "primary") as "danger" | "primary" }] : []),
                  ],
                  onPress: () => openProject(p.id),
                  chips: <StatusChip status={healthChip(p.status)} label={p.status === "gaps" ? t("snapshot.status.gaps", { count: p.empty_places }) : t(`snapshot.status.${p.status}`)} testID={`status-${p.id}`} />,
                }))}
              />
            )}
          </View>
          {d.terms_estimated ? (
            <View testID="note-estimated" style={[styles.note, { borderColor: colors.border.strong }]}>
              <Txt role="meta" tone="secondary">
                {t("snapshot.estimated_note")}
              </Txt>
            </View>
          ) : null}
        </View>
      )}
    </Screen>
  );
}

const styles = StyleSheet.create({
  body: { gap: 28 },
  chart: { flex: 1, gap: 12, minWidth: 0 },
  chartWide: { flex: 1.4, gap: 8, minWidth: 0 },
  table: { gap: 8 },
  nameCell: { minWidth: 0 },
  coverageCell: { alignItems: "flex-end" },
  note: { borderTopWidth: 1, paddingTop: 8 },
});
