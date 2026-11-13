/**
 * What a proposal guarantees, what it traded off, its chips, and how fair it is (specs/008-auto-
 * scheduler-review FR-023 to FR-026). Read again whenever the proposal's version moves, so it follows
 * every edit at once. Numbers come from the server; the words are the locale files'. Each person's
 * hours are a bar against the average; above it reads in a quiet tone (no warnings); no score.
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { Button, InlineBanner, SkeletonRows, Txt } from "../../design/components";
import { Icon } from "../../design/icons/Icon";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { useLoad } from "../useLoad";
import { hours } from "./review";
import { average, chipWords, type Chip } from "./steering";

interface Props {
  jobId: string;
  /** The proposal's version: a new one reads the panel again (FR-026). */
  version: number;
  name: (id: string) => string;
  place: (id: string) => string;
  type: (id: string) => string;
}

/** How many people the fairness list shows before "show all". */
const FAIRNESS_TOP = 10;

export function FairnessPanel({ jobId, version, name, place, type }: Props) {
  const { t, i18n } = useTranslation();
  const [showAll, setShowAll] = useState(false);
  const { colors } = useTheme();
  const session = useSession();
  const errorText = useErrorText();
  const loaded = useLoad(
    async () => call(session.api.GET("/api/v1/schedule-jobs/{id}/fairness", { params: { path: { id: jobId } } })),
    [jobId, version],
  );
  if (loaded.error && !loaded.data) return <InlineBanner tone="warning" title={errorText(loaded.error)} action={{ label: t("common.retry"), onPress: loaded.reload }} />;
  if (!loaded.data) return <SkeletonRows rows={3} />;
  const data = loaded.data;
  const traded = data.tradeoffs;
  const totals = data.totals;
  const lang = i18n.language;
  const summary = [
    traded.empty_places > 0 ? t("review.summary.empty", { count: traded.empty_places }) : t("review.summary.all_filled"),
    t("review.summary.wishes", { missed: traded.open_wishes_missed, given: traded.close_wishes_given }),
    t("review.summary.hours", { low: hours(traded.minutes_low, lang), high: hours(traded.minutes_high, lang) }),
    t("review.summary.nights", { low: traded.nights_low, high: traded.nights_high }),
    ...(data.chips.length > 0 ? [t("review.summary.chips", { count: traded.chips_not_met, total: data.chips.length })] : []),
  ];
  const sortedShares = [...data.shares].sort((a, b) => b.minutes - a.minutes);
  const largest = Math.max(1, ...data.shares.map((x) => x.minutes));
  const averageMinutes = totals.people > 0 ? totals.minutes / totals.people : 0;
  const chipState = (state: string, cause: string | null | undefined) =>
    state === "met" ? t("review.chip.met") : state === "not_applying" ? t("review.chip.not_applying") : t(`review.chip.not_met_${cause ?? "coverage"}`);

  return (
    <View style={styles.root} testID="fairness-panel">
      {/* What the proposal keeps, always (FR-023). */}
      <View style={styles.line} testID="review-guarantee">
        <Icon name="check" size={16} color={colors.status.success} />
        <Txt role="body" style={styles.grow}>
          {t("review.guarantee")}
        </Txt>
      </View>
      {data.locks > 0 ? (
        <Txt role="meta" tone="secondary" testID="review-locks">
          {t("review.locks_kept", { count: data.locks })}
        </Txt>
      ) : null}

      <Txt role="label" heading={3}>
        {t("review.summary_title")}
      </Txt>
      <View testID="review-summary">
        {summary.map((line) => (
          <Txt key={line} role="body" numeric>
            {line}
          </Txt>
        ))}
      </View>

      {data.chips.length > 0 ? (
        <View style={styles.list} testID="review-chips">
          {data.chips.map((chip, index) => {
            const said = chipWords(chip as Chip, name, place, type, lang);
            const met = chip.state === "met";
            return (
              <View key={index} style={styles.line} testID="review-chip" {...webProps({ dataSet: { state: chip.state } })}>
                <Icon name={met ? "check" : "info"} size={16} color={met ? colors.status.success : colors.text.secondary} />
                <Txt role="body" style={styles.grow}>
                  {`${t(said.key, said.values)}: ${chipState(chip.state, chip.cause)}`}
                </Txt>
              </View>
            );
          })}
        </View>
      ) : null}

      {/* Each person against the run's averages (FR-025): outliers carry an icon and words. */}
      <Txt role="label" heading={3}>
        {t("review.fairness_title")}
      </Txt>
      <Txt role="meta" tone="secondary" numeric testID="fairness-averages">
        {t("review.fairness_average", {
          hours: hours(totals.people > 0 ? totals.minutes / totals.people : 0, lang),
          nights: average(totals.nights, totals.people),
          weekends: average(totals.weekends, totals.people),
        })}
      </Txt>
      {/* Each person's hours as a bar against the average, most first; the top ten until asked
          (the owner, 2026-11-13). Above the average reads in a quiet tone: no warnings. */}
      <View style={styles.list} testID="fairness-bars">
        {(showAll ? sortedShares : sortedShares.slice(0, FAIRNESS_TOP)).map((share) => {
          const outliers = [
            share.hours_outlier ? t("review.above.hours") : null,
            share.nights_outlier ? t("review.above.nights") : null,
            share.weekends_outlier ? t("review.above.weekends") : null,
          ].filter(Boolean);
          const width = `${(share.minutes / largest) * 100}%` as const;
          return (
            <View key={share.user_id} style={styles.barRow} testID="fairness-row" {...webProps({ dataSet: { userId: share.user_id, outlier: String(outliers.length > 0) } })}>
              <View style={styles.barHead}>
                <Txt role="label" numberOfLines={1} style={styles.grow}>
                  {name(share.user_id)}
                </Txt>
                <Txt role="meta" tone="secondary" numeric>
                  {t("review.fairness_share", { hours: hours(share.minutes, lang), nights: share.nights, weekends: share.weekends })}
                </Txt>
              </View>
              <View style={[styles.track, { backgroundColor: colors.bg.selected }]}>
                <View style={[styles.fill, { width, backgroundColor: share.hours_outlier ? colors.text.primary : colors.border.strong }]} />
                {/* The average, a thin mark across the track. */}
                <View style={[styles.average, { start: `${(averageMinutes / largest) * 100}%`, backgroundColor: colors.accent.default }]} />
              </View>
              {outliers.length > 0 ? (
                <Txt role="meta" tone="secondary" testID="fairness-outlier">
                  {outliers.join(" · ")}
                </Txt>
              ) : null}
            </View>
          );
        })}
      </View>
      {sortedShares.length > FAIRNESS_TOP ? (
        <Button
          variant="quiet"
          icon={showAll ? "chevron-up" : "chevron-down"}
          label={showAll ? t("review.fairness_top", { count: FAIRNESS_TOP }) : t("review.fairness_all", { count: sortedShares.length })}
          onPress={() => setShowAll(!showAll)}
          testID="fairness-show-all"
        />
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  root: { gap: 8 },
  line: { flexDirection: "row", alignItems: "center", gap: 6 },
  grow: { flexShrink: 1 },
  list: { gap: 6 },
  barRow: { gap: 4 },
  barHead: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 8 },
  track: { height: 8, position: "relative" },
  fill: { height: 8 },
  average: { position: "absolute", top: -3, width: 2, height: 14 },
});
