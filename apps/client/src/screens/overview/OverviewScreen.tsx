/**
 * The snapshot dashboard (specs/demo-ceo/plan.md, screen 1; deck slide 14), where an Admin or a
 * Manager lands: a month, quarter or year switch; four numbered key figures (billing so far, the
 * forecast to the period's end, this week's coverage, people); a dark panel for temporary workers;
 * weekly bars of billing against pay with the forecast weeks lighter; and the project table with
 * coverage, margin and a status chip. Every amount is the API's own; pay shows only where sent.
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { FeaturePanel, InlineBanner, LinkPressable, PageTitle, SegmentedControl, SkeletonRows, StatusChip, Table, Txt, type Column } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { DEFAULT_TIME_ZONE, formatMoney, todayIn } from "../../locale";
import { formatHoursFigure } from "../reports/HoursCostsScreen";
import { ColumnBars, KeyFigures, SectionHead, Split, type BarColumn, type KeyFigure } from "../deck/parts";
import { useLoad } from "../useLoad";
import { coveragePercent, formatPercent, healthChip, periodRange, weekLabel, type OverviewProject, type Period } from "./model";

export function OverviewScreen() {
  const { t, i18n } = useTranslation();
  const { api } = useSession();
  const errorText = useErrorText();
  const { colors } = useTheme();
  const today = todayIn(DEFAULT_TIME_ZONE);
  const [period, setPeriod] = useState<Period>("month");
  const range = periodRange(period, today);
  const overview = useLoad(async () => call(api.GET("/api/v1/reports/overview", { params: { query: range } })), [range.from, range.to]);
  const money = (amount: string) => formatMoney(amount, i18n.language);
  const d = overview.data;

  const title =
    period === "month"
      ? new Intl.DateTimeFormat(i18n.language === "he" ? "he-IL" : "en-GB", { month: "long", year: "numeric", timeZone: "UTC" }).format(new Date(`${range.from}T12:00:00Z`))
      : period === "quarter"
        ? t("snapshot.quarter_title", { quarter: Math.floor((Number(range.from.slice(5, 7)) - 1) / 3) + 1, year: range.from.slice(0, 4) })
        : range.from.slice(0, 4);

  const coverage = d ? coveragePercent(d.coverage) : null;
  const figures: KeyFigure[] = d
    ? [
        // Where pay is hidden the API sends no money at all: the first two figures are hours then.
        ...(d.billing_actual != null && d.billing_forecast != null
          ? [
              {
                key: "billing",
                label: t("snapshot.figure.billing"),
                value: money(d.billing_actual),
                raw: d.billing_actual,
                detail: d.pay_actual != null ? t("snapshot.figure.billing_detail", { pay: money(d.pay_actual), hours: formatHoursFigure(d.hours_actual, i18n.language) }) : t("snapshot.figure.billing_hours", { hours: formatHoursFigure(d.hours_actual, i18n.language) }),
              },
              {
                key: "forecast",
                label: t(`snapshot.figure.forecast.${period}`),
                value: money(d.billing_forecast),
                raw: d.billing_forecast,
                detail: d.margin_pct != null ? t("snapshot.figure.forecast_margin", { margin: formatPercent(d.margin_pct) }) : t("snapshot.figure.forecast_detail"),
                tone: (d.margin_pct != null && Number(d.margin_pct) < 0 ? "danger" : "primary") as KeyFigure["tone"],
              },
            ]
          : [
              { key: "hours", label: t("snapshot.figure.hours"), value: formatHoursFigure(d.hours_actual, i18n.language), raw: d.hours_actual, detail: t("snapshot.figure.hours_detail") },
              { key: "forecast", label: t(`snapshot.figure.forecast_hours.${period}`), value: formatHoursFigure(d.hours_forecast, i18n.language), raw: d.hours_forecast, detail: t("snapshot.figure.forecast_detail") },
            ]),
        {
          key: "coverage",
          label: t("snapshot.figure.coverage"),
          value: coverage === null ? "—" : `${coverage}%`,
          raw: coverage === null ? "" : String(coverage),
          detail: d.coverage.empty_places === 0 ? t("snapshot.figure.coverage_full") : t("snapshot.figure.coverage_detail", { count: d.coverage.empty_places, projects: d.coverage.projects_with_gaps }),
        },
        {
          key: "people",
          label: t("snapshot.figure.people"),
          value: String(d.people.working_this_week),
          unit: t("snapshot.figure.people_unit", { active: d.people.active }),
          raw: String(d.people.working_this_week),
          detail: t("snapshot.figure.people_detail", { count: d.people.near_weekly_limit }),
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
    { key: "coverage", title: t("snapshot.col.coverage"), numeric: true, width: 90, render: (p) => <Txt role="body" numeric>{formatPercent(p.coverage_pct)}</Txt> },
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
          <KeyFigures figures={figures} />
          <Split>
            <View style={styles.bars}>
              <SectionHead title={showPay ? t("snapshot.bars_title") : showBilling ? t("snapshot.bars_title_billing") : t("snapshot.bars_title_hours")} />
              <ColumnBars columns={bars} legend={showPay ? t("snapshot.legend") : t("snapshot.legend_billing")} />
            </View>
            {d.people.temporary > 0 ? (
              <View style={styles.panel}>
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
            <Table columns={columns} rows={d.projects} keyOf={(p) => p.id} caption={t("snapshot.projects_title")} testID="overview-projects" />
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
  bars: { flex: 1.8, gap: 12, minWidth: 0 },
  panel: { flex: 1, minWidth: 0 },
  table: { gap: 8 },
  nameCell: { minWidth: 0 },
  note: { borderTopWidth: 1, paddingTop: 8 },
});
