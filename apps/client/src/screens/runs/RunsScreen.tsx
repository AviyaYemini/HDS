/**
 * Schedule runs (specs/007-ux-redesign FR-042, T079): the organisation's runs, newest first, each with
 * its range, status and how good its proposal is; and the generate form, a range that cannot start in
 * the past chosen from the planning presets or the calendar. Generating opens the run.
 */
import { useRouter } from "expo-router";
import { useCallback, useEffect, useState } from "react";
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
import { DEFAULT_TIME_ZONE, addDays, compareDates, formatNumericRange, presetRange, todayIn, type DateRange } from "../../locale";
import { HonestyLabel } from "./HonestyLabel";
import { STATUS_TONE, isActive, type JobSummary } from "./model";

/** A run covers at most this many days (the API's limit, 004 FR-005). */
const MAX_DAYS = 31;
const REFRESH_MS = 2000;


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
      <StatusChip status={STATUS_TONE[job.status]} label={t(`runs.status.${job.status}`)} />
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

  const tooLong = compareDates(range.end, addDays(range.start, MAX_DAYS - 1)) > 0;

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
        <Button variant="primary" icon="play" label={t("runs.generate")} onPress={() => void generate()} busy={busy} disabled={tooLong} disabledReason={tooLong ? t("schedule.range_invalid") : undefined} testID="action-generate" />
        <Button label={t("common.cancel")} onPress={onCancel} testID="action-cancel-run" />
      </View>
    </View>
  );
}

/**
 * The runs, newest first: the first page, and the next on request. While a run on screen is queued or
 * running, the first page is read again in the background, so its status keeps up.
 */
function useRuns() {
  const session = useSession();
  const [items, setItems] = useState<JobSummary[] | null>(null);
  const [cursor, setCursor] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);

  // State is set in the answer's callbacks, never in an effect's own body.
  const first = useCallback(
    (background: boolean) =>
      call((background ? session.quiet : session.api).GET("/api/v1/schedule-jobs", { params: { query: { limit: 50 } } })).then(
        (page) => {
          setItems((held) => {
            const fresh = new Set(page.items.map((job) => job.id));
            return [...page.items, ...(held ?? []).filter((job) => !fresh.has(job.id))];
          });
          // Only a first read sets where the next page starts; a refresh keeps the list's end.
          if (!background) setCursor(page.next_cursor);
          setError(null);
        },
        (failure: unknown) => {
          if (!background) setError(codeOf(failure));
        },
      ),
    [session],
  );

  useEffect(() => {
    void first(false);
  }, [first]);

  const running = (items ?? []).some(isActive);
  useEffect(() => {
    if (!running) return undefined;
    const timer = setInterval(() => void first(true), REFRESH_MS);
    return () => clearInterval(timer);
  }, [running, first]);

  const more = async () => {
    if (cursor === null) return;
    setLoadingMore(true);
    try {
      const page = await call(session.api.GET("/api/v1/schedule-jobs", { params: { query: { limit: 50, cursor } } }));
      setItems((held) => {
        const ids = new Set((held ?? []).map((job) => job.id));
        return [...(held ?? []), ...page.items.filter((job) => !ids.has(job.id))];
      });
      setCursor(page.next_cursor);
    } catch (failure) {
      setError(codeOf(failure));
    } finally {
      setLoadingMore(false);
    }
  };

  return { data: items, error, reload: () => first(false), more: cursor === null ? null : more, loadingMore };
}

export function RunsScreen() {
  const { t } = useTranslation();
  const errorText = useErrorText();
  const [creating, setCreating] = useState(false);
  const jobs = useRuns();
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
          {jobs.more ? <Button label={t("runs.more")} onPress={() => void jobs.more?.()} busy={jobs.loadingMore} testID="action-more-runs" /> : null}
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
