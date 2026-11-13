/**
 * Hours and costs in the deck's style (specs/demo-ceo/plan.md, screen 4; deck slide 9): numbered
 * key figures, vertical bars per group (security dark, training light when grouped by project) and
 * the table. Every figure is the API's own: hours, shifts, billing and gross pay, each shown only
 * when the API sends it (billing arrives with the overview; pay only to those allowed to see it).
 */
import { useRouter } from "expo-router";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { InlineBanner, PageTitle, SegmentedControl, StatusChip, Table, Txt, type Column } from "../../design/components";
import { useTheme } from "../../design/theme/ThemeProvider";
import { Screen } from "../../design/components/Screen";
import { DEFAULT_TIME_ZONE, formatMoney, formatNumericRange, presetRange, todayIn } from "../../locale";
import { CardList, DashboardSkeleton, HBars, KeyFigures, SectionHead, type HBar, type KeyFigure } from "../deck/parts";
import { formatMoneyWhole } from "../deck/polish";
import { useEntrance } from "../deck/useEntrance";
import { formatPercent } from "../overview/model";
import { rosterHref } from "../roster/ProjectChooser";
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

/**
 * A group's margin, (billing − pay) ÷ billing, as a "12.50" percent; null without both or with no
 * billing. A percentage of the API's amounts, for reading, never an amount itself.
 */
export function marginOf(g: { billing?: string | null; pay?: string | null; shifts_missing_rate?: number }): string | null {
  // A total that leaves out shifts with no rate would read their cost as zero: no margin then.
  if (g.billing == null || g.pay == null || (g.shifts_missing_rate ?? 0) > 0) return null;
  const billing = Number(g.billing);
  if (billing <= 0) return null;
  return (((billing - Number(g.pay)) / billing) * 100).toFixed(2);
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
  const router = useRouter();
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

  // Whole shekels in the figures, counting up on entrance; the table keeps its agorot.
  const whole = (amount: string) => formatMoneyWhole(amount, i18n.language);
  const countMoney = (n: number) => formatMoneyWhole(Math.round(n).toFixed(2), i18n.language);
  const progress = useEntrance(data ? `${data.from}|${data.to}|${groupBy}` : null);
  const figures: KeyFigure[] = data
    ? [
        { key: "hours", label: t("hc.figure.hours"), value: hours(data.hours), raw: data.hours, count: { to: Number(data.hours), format: (n: number) => hours(n.toFixed(2)) }, detail: t("hc.figure.hours_detail", { count: data.shifts }) },
        ...(showBilling ? [{ key: "billing", label: t("hc.figure.billing"), value: whole(data.billing!), raw: data.billing!, count: { to: Number(data.billing), format: countMoney }, detail: t("hc.figure.billing_detail") }] : []),
        ...(showPay
          ? [{ key: "pay", label: t("hc.figure.pay"), value: whole(data.pay!), raw: data.pay!, count: { to: Number(data.pay), format: countMoney }, detail: data.shifts_missing_rate > 0 ? t("pay.missing_count", { count: data.shifts_missing_rate }) : t("hc.figure.pay_detail") }]
          : []),
        ...(showBilling && showPay && marginOf(data) !== null
          ? [{ key: "margin", label: t("hc.figure.margin"), value: formatPercent(marginOf(data)), raw: marginOf(data)!, detail: t("hc.figure.margin_detail") }]
          : [{ key: "groups", label: t(`hc.figure.groups.${groupBy}`), value: String(groups.length), raw: String(groups.length), detail: t("hc.figure.groups_detail") }]),
      ]
    : [];

  /** A project's row opens its roster week; people and clients open nothing. */
  const open = groupBy === "project" ? (id: string) => router.push(rosterHref(id) as never) : null;
  const typeOf = (id: string) => projects.data?.find((p) => p.id === id)?.type;
  const hbars: HBar[] = topGroups(groups, isDesktop ? 10 : 6).map((g) => ({
    key: g.id,
    label: g.name,
    value: Number(g.hours),
    valueText: t("pay.hours", { hours: hours(g.hours) }),
    light: groupBy === "project" && typeOf(g.id) === "training",
    ...(open ? { onPress: () => open(g.id) } : {}),
  }));

  const missing = (g: Group) => (g.shifts_missing_rate > 0 ? t("hc.missing_rate", { count: g.shifts_missing_rate }) : "—");
  const tableColumns: Column<Group>[] = [
    { key: "name", title: t(`hc.col.${groupBy}`), render: (g) => <Txt role="label">{g.name}</Txt> },
    { key: "hours", title: t("hc.col.hours"), numeric: true, width: 90, render: (g) => <Txt role="body" numeric>{hours(g.hours)}</Txt> },
    { key: "shifts", title: t("hc.col.shifts"), numeric: true, width: 90 },
    ...(showBilling ? [{ key: "billing", title: t("hc.col.billing"), numeric: true, width: 140, render: (g: Group) => <Txt role="body" numeric>{g.billing != null ? money(g.billing) : "—"}</Txt> }] : []),
    ...(showPay
      ? [{ key: "pay", title: t("hc.col.pay"), numeric: true, width: 170, render: (g: Group) => (g.pay != null && g.shifts_missing_rate === 0 ? <Txt role="body" numeric>{money(g.pay)}</Txt> : <Txt role="meta" tone="secondary">{g.pay != null ? `${money(g.pay)} · ${missing(g)}` : missing(g)}</Txt>) }]
      : []),
    ...(showBilling && showPay
      ? [{ key: "margin", title: t("hc.col.margin"), numeric: true, width: 90, render: (g: Group) => { const m = marginOf(g); return <Txt role="body" numeric tone={m !== null && Number(m) < 0 ? "danger" : "primary"}>{formatPercent(m)}</Txt>; } }]
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
        <DashboardSkeleton charts={1} chartHeight={260} rows={6} />
      ) : (
        <View style={styles.body}>
          <Txt role="meta" tone="secondary" numeric>
            {`${formatNumericRange(data.from, data.to)} · ${t("hc.counted")}`}
          </Txt>
          <KeyFigures figures={figures} progress={progress} />
          {groups.length === 0 ? (
            <Txt role="body" tone="secondary">
              {t("reports.nothing_in_range")}
            </Txt>
          ) : (
            <>
              <View style={styles.chart}>
                <SectionHead title={t(`hc.bars.${groupBy}`)} />
                <HBars bars={hbars} progress={progress} testID="hc-bars" legend={groupBy === "project" ? t("hc.legend_project", { count: hbars.length }) : t("hc.legend", { count: hbars.length })} />
              </View>
              {/* The table at full width, with a wide name column (the owner, 2026-11-13). */}
              <View style={styles.table}>
                {isDesktop ? (
                  <Table columns={tableColumns} rows={shown} keyOf={(g) => g.id} caption={t("hc.title")} testID="hc-table" {...(open ? { onRowPress: (g: Group) => open(g.id) } : {})} />
                ) : (
                  // A phone: a card a group, its hours, money and margin, and a chip for missing rates.
                  <CardList
                    testID="hc-table"
                    items={shown.map((g) => ({
                      key: g.id,
                      title: g.name,
                      ...(open ? { onPress: () => open(g.id) } : {}),
                      figures: [
                        { label: t("hc.col.hours"), value: hours(g.hours) },
                        ...(showBilling && g.billing != null ? [{ label: t("hc.col.billing"), value: money(g.billing) }] : [{ label: t("hc.col.shifts"), value: String(g.shifts) }]),
                        ...(showPay && g.pay != null ? [{ label: t("hc.col.pay"), value: money(g.pay) }] : []),
                        ...(marginOf(g) !== null ? [{ label: t("hc.col.margin"), value: formatPercent(marginOf(g)) }] : []),
                      ],
                      chips: g.shifts_missing_rate > 0 ? <StatusChip status="warning" label={missing(g)} /> : undefined,
                    }))}
                  />
                )}
                {pager}
              </View>
            </>
          )}
        </View>
      )}
    </Screen>
  );
}

const styles = StyleSheet.create({
  controls: { flexDirection: "row", flexWrap: "wrap", gap: 16, alignItems: "flex-end" },
  body: { gap: 24 },
  chart: { gap: 12 },
  table: { gap: 8 },
});
