/**
 * Hours and costs in the deck's style (specs/demo-ceo/plan.md, screen 4; deck slide 9): numbered
 * key figures, vertical bars per group (security dark, training light when grouped by project) and
 * the table. Every figure is the API's own: hours, shifts, billing and gross pay, each shown only
 * when the API sends it (billing arrives with the overview; pay only to those allowed to see it).
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { InlineBanner, PageTitle, SegmentedControl, SkeletonRows, StatusChip, Table, Txt, type Column } from "../../design/components";
import { useTheme } from "../../design/theme/ThemeProvider";
import { Screen } from "../../design/components/Screen";
import { DEFAULT_TIME_ZONE, formatMoney, formatNumericRange, presetRange, todayIn } from "../../locale";
import { CardList, ColumnBars, KeyFigures, SectionHead, Split, type BarColumn, type KeyFigure } from "../deck/parts";
import { useLoad } from "../useLoad";
import { usePage, useReportRange } from "./parts";

type GroupBy = "project" | "employee" | "client";

const HC_PRESETS = ["this-pay-period", "last-pay-period", "last-30-days"] as const;
type HcPreset = (typeof HC_PRESETS)[number];

/** A group or total as the API answers it; `billing` is read defensively until the field lands. */
interface Figures {
  hours: string;
  shifts: number;
  pay?: string | null;
  billing?: string | null;
  shifts_missing_rate: number;
}
interface Group extends Figures {
  id: string;
  name: string;
}

/** Hours as a big figure: "1,984" or "1,984.5", never more than one decimal. */
export function formatHoursFigure(hours: string, locale: string): string {
  return new Intl.NumberFormat(locale === "he" ? "he-IL" : "en-GB", { maximumFractionDigits: 1 }).format(Number(hours));
}

/** The largest groups for the bars, at most `limit`, the biggest first. */
export function topGroups<T extends { hours: string }>(groups: readonly T[], limit = 8): T[] {
  return [...groups].sort((a, b) => Number(b.hours) - Number(a.hours)).slice(0, limit);
}

export function HoursCostsScreen() {
  const { t, i18n } = useTranslation();
  const { api } = useSession();
  const errorText = useErrorText();
  const { isDesktop } = useTheme();
  const { setRange, asked } = useReportRange();
  const today = todayIn(DEFAULT_TIME_ZONE);
  // The deck's period buttons (slide 9) instead of an open calendar: the report's first screen is its figures.
  const [preset, setPreset] = useState<HcPreset>("this-pay-period");
  const choose = (next: HcPreset) => {
    setPreset(next);
    setRange(presetRange(next, today));
  };
  const [groupBy, setGroupBy] = useState<GroupBy>("project");
  const report = useLoad(async () => call(api.GET("/api/v1/reports/hours", { params: { query: { ...asked, group_by: groupBy } } })), [asked.from, asked.to, groupBy]);
  // A project's type colours its bar (security dark, training light).
  const projects = useLoad(async () => (groupBy === "project" ? (await call(api.GET("/api/v1/projects"))).items : []), [groupBy]);
  const money = (amount: string) => formatMoney(amount, i18n.language);
  const hours = (value: string) => formatHoursFigure(value, i18n.language);

  const data = report.data as (Figures & { groups: Group[]; from: string; to: string }) | null;
  const groups = data?.groups ?? [];
  const showPay = data?.pay != null;
  const showBilling = data?.billing != null;
  const { shown, pager } = usePage(groups, `${groupBy}|${asked.from}|${asked.to}`);

  const figures: KeyFigure[] = data
    ? [
        { key: "hours", label: t("hc.figure.hours"), value: hours(data.hours), raw: data.hours, detail: t("hc.figure.hours_detail", { count: data.shifts }) },
        ...(showBilling ? [{ key: "billing", label: t("hc.figure.billing"), value: money(data.billing!), raw: data.billing!, detail: t("hc.figure.billing_detail") }] : []),
        ...(showPay
          ? [{ key: "pay", label: t("hc.figure.pay"), value: money(data.pay!), raw: data.pay!, detail: data.shifts_missing_rate > 0 ? t("pay.missing_count", { count: data.shifts_missing_rate }) : t("hc.figure.pay_detail") }]
          : []),
        { key: "groups", label: t(`hc.figure.groups.${groupBy}`), value: String(groups.length), raw: String(groups.length), detail: t("hc.figure.groups_detail") },
      ]
    : [];

  const typeOf = (id: string) => projects.data?.find((p) => p.id === id)?.type;
  const columns: BarColumn[] = topGroups(groups, isDesktop ? 8 : 5).map((g) => ({
    key: g.id,
    label: g.name,
    dark: groupBy === "project" && typeOf(g.id) === "training" ? 0 : Number(g.hours),
    light: groupBy === "project" && typeOf(g.id) === "training" ? Number(g.hours) : null,
    title: `${g.name}: ${t("pay.hours", { hours: g.hours })}`,
  }));

  const missing = (g: Group) => (g.shifts_missing_rate > 0 ? t("hc.missing_rate", { count: g.shifts_missing_rate }) : "—");
  const tableColumns: Column<Group>[] = [
    { key: "name", title: t(`hc.col.${groupBy}`), render: (g) => <Txt role="label">{g.name}</Txt> },
    { key: "hours", title: t("hc.col.hours"), numeric: true, render: (g) => <Txt role="body" numeric>{hours(g.hours)}</Txt> },
    { key: "shifts", title: t("hc.col.shifts"), numeric: true },
    ...(showBilling ? [{ key: "billing", title: t("hc.col.billing"), numeric: true, render: (g: Group) => <Txt role="body" numeric>{g.billing != null ? money(g.billing) : "—"}</Txt> }] : []),
    ...(showPay
      ? [{ key: "pay", title: t("hc.col.pay"), numeric: true, render: (g: Group) => (g.pay != null && g.shifts_missing_rate === 0 ? <Txt role="body" numeric>{money(g.pay)}</Txt> : <Txt role="meta" tone="secondary">{g.pay != null ? `${money(g.pay)} · ${missing(g)}` : missing(g)}</Txt>) }]
      : []),
  ];

  return (
    <Screen testID="screen-hours-costs">
      <PageTitle eyebrow={[t("app.name"), t("nav.reports")]} title={t("hc.title")} />
      <View style={styles.controls}>
        <SegmentedControl<HcPreset>
          label={t("reports.range_label")}
          options={HC_PRESETS.map((p) => ({ value: p, label: t(`hc.preset.${p}`), testID: `preset-${p}` }))}
          value={preset}
          onChange={choose}
        />
        <SegmentedControl<GroupBy>
          label={t("reports.group_by")}
          options={(["project", "employee", "client"] as const).map((g) => ({ value: g, label: t(`reports.by_${g}`), testID: `group-${g}` }))}
          value={groupBy}
          onChange={setGroupBy}
        />
      </View>
      {report.error ? (
        <InlineBanner tone="warning" title={errorText(report.error)} action={{ label: t("common.retry"), onPress: () => void report.reload() }} />
      ) : data === null ? (
        <SkeletonRows rows={5} />
      ) : (
        <View style={styles.body}>
          <Txt role="meta" tone="secondary" numeric>
            {`${formatNumericRange(data.from, data.to)} · ${t("hc.counted")}`}
          </Txt>
          <KeyFigures figures={figures} />
          {groups.length === 0 ? (
            <Txt role="body" tone="secondary">
              {t("reports.nothing_in_range")}
            </Txt>
          ) : (
            <Split>
              <View style={styles.chart}>
                <SectionHead title={t(`hc.bars.${groupBy}`)} />
                <ColumnBars columns={columns} legend={groupBy === "project" ? t("hc.legend_project", { count: isDesktop ? 8 : 5 }) : t("hc.legend", { count: isDesktop ? 8 : 5 })} />
              </View>
              <View style={styles.table}>
                {isDesktop ? (
                  <Table columns={tableColumns} rows={shown} keyOf={(g) => g.id} caption={t("hc.title")} testID="hc-table" />
                ) : (
                  // A phone: a card a group, its hours, shifts and money, and a chip for missing rates.
                  <CardList
                    testID="hc-table"
                    items={shown.map((g) => ({
                      key: g.id,
                      title: g.name,
                      figures: [
                        { label: t("hc.col.hours"), value: hours(g.hours) },
                        ...(showBilling && g.billing != null ? [{ label: t("hc.col.billing"), value: money(g.billing) }] : [{ label: t("hc.col.shifts"), value: String(g.shifts) }]),
                        ...(showPay && g.pay != null ? [{ label: t("hc.col.pay"), value: money(g.pay) }] : []),
                      ],
                      chips: g.shifts_missing_rate > 0 ? <StatusChip status="warning" label={missing(g)} /> : undefined,
                    }))}
                  />
                )}
                {pager}
              </View>
            </Split>
          )}
        </View>
      )}
    </Screen>
  );
}

const styles = StyleSheet.create({
  controls: { flexDirection: "row", flexWrap: "wrap", gap: 16, alignItems: "flex-end" },
  body: { gap: 24 },
  chart: { flex: 1, gap: 12, minWidth: 0 },
  table: { flex: 1.5, gap: 8, minWidth: 0 },
});
