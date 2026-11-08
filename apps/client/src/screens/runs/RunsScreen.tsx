/**
 * Schedule runs (specs/007-ux-redesign FR-042, T079): the organisation's runs, newest first, each with
 * its range, status and how good its proposal is; and the generate form, a range that cannot start in
 * the past chosen from the planning presets or the calendar. Generating opens the run.
 */
import { useRouter } from "expo-router";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import {
  Button,
  DateRangeField,
  EmptyState,
  InlineBanner,
  LinkPressable,
  PageTitle,
  SkeletonRows,
  StatusChip,
  Txt,
} from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { DEFAULT_TIME_ZONE, formatNumericRange, presetRange, todayIn, type DateRange } from "../../locale";
import { useLoad } from "../useLoad";
import { HonestyLabel } from "./HonestyLabel";
import type { JobSummary } from "./model";

const TONE = { queued: "neutral", running: "info", succeeded: "success", failed: "danger" } as const;

function RunRow({ job }: { job: JobSummary }) {
  const { t } = useTranslation();
  const { colors } = useTheme();
  return (
    <LinkPressable
      href={`/runs/${job.id}`}
      testID="run-row"
      {...webProps({ dataSet: { jobId: job.id, status: job.status } })}
      style={[styles.row, { borderColor: colors.border.subtle }]}
    >
      <Txt role="label" numeric style={styles.range}>
        {formatNumericRange(job.from, job.to)}
      </Txt>
      <StatusChip status={TONE[job.status]} label={t(`runs.status.${job.status}`)} />
      <HonestyLabel job={job} testID="run-row-honesty" />
      {job.proposal_state && job.status === "succeeded" ? (
        <Txt role="meta" tone="secondary">
          {t(`runs.proposal.${job.proposal_state}`)}
        </Txt>
      ) : null}
    </LinkPressable>
  );
}

function newKey(): string {
  return `run-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

function GenerateForm({ onCancel }: { onCancel: () => void }) {
  const { t } = useTranslation();
  const session = useSession();
  const router = useRouter();
  const errorText = useErrorText();
  const { colors } = useTheme();
  const today = todayIn(DEFAULT_TIME_ZONE);
  const [range, setRange] = useState<DateRange>(() => presetRange("next-week", today));
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // One key per range: a repeat (a double press, a retry after a lost answer) answers the same run,
  // and a new range is a new request.
  const [key, setKey] = useState(newKey);
  const choose = (next: DateRange) => {
    setRange(next);
    setKey(newKey());
  };

  const generate = async () => {
    setBusy(true);
    setError(null);
    try {
      const job = await call(
        session.api.POST("/api/v1/schedule-jobs", {
          body: { from: range.start, to: range.end, solver: "cpsat" },
          params: { header: { "idempotency-key": key } },
        }),
      );
      router.push(`/runs/${job.id}`);
    } catch (failure) {
      setError(codeOf(failure));
      setBusy(false);
    }
  };

  return (
    <View testID="run-form" style={[styles.form, { borderColor: colors.border.subtle, backgroundColor: colors.bg.surface }]}>
      <DateRangeField label={t("runs.range")} value={range} onChange={choose} forward minDate={today} />
      <Txt role="meta" tone="secondary">
        {t("runs.range_hint")}
      </Txt>
      {error ? <InlineBanner tone="warning" title={errorText(error)} /> : null}
      <View style={styles.formActions}>
        <Button variant="primary" icon="play" label={t("runs.generate")} onPress={() => void generate()} busy={busy} testID="action-generate" />
        <Button label={t("common.cancel")} onPress={onCancel} testID="action-cancel-run" />
      </View>
    </View>
  );
}

export function RunsScreen() {
  const { t } = useTranslation();
  const session = useSession();
  const errorText = useErrorText();
  const [creating, setCreating] = useState(false);
  const jobs = useLoad(async () => (await call(session.api.GET("/api/v1/schedule-jobs", { params: { query: { limit: 50 } } }))).items);
  return (
    <Screen testID="screen-runs">
      <PageTitle eyebrow={[t("app.name"), t("nav.group.manage")]} title={t("nav.runs")}>
        {creating ? null : <Button variant="primary" icon="plus" label={t("runs.new")} onPress={() => setCreating(true)} testID="action-new-run" />}
      </PageTitle>
      {creating ? <GenerateForm onCancel={() => setCreating(false)} /> : null}
      {jobs.error ? (
        <InlineBanner tone="warning" title={errorText(jobs.error)} action={{ label: t("common.retry"), onPress: () => void jobs.reload() }} />
      ) : jobs.data === null ? (
        <SkeletonRows rows={4} />
      ) : jobs.data.length === 0 ? (
        creating ? null : (
          <EmptyState
            icon="play"
            title={t("runs.empty_title")}
            body={t("runs.empty_body")}
            action={{ label: t("runs.new"), onPress: () => setCreating(true), testID: "action-new-run-empty" }}
          />
        )
      ) : (
        <View style={styles.list}>
          {jobs.data.map((job) => (
            <RunRow key={job.id} job={job} />
          ))}
        </View>
      )}
    </Screen>
  );
}

const styles = StyleSheet.create({
  list: { gap: 8 },
  row: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 12, padding: 12, borderWidth: 1 },
  range: { flexGrow: 1, minWidth: 160 },
  form: { gap: 12, padding: 16, borderWidth: 1 },
  formActions: { flexDirection: "row", flexWrap: "wrap", gap: 8 },
});
