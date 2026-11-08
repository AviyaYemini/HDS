/**
 * One schedule run (specs/007-ux-redesign FR-042, FR-074, T079). While it runs, the loading bar is
 * determinate: elapsed time against the time limit, labelled "up to N s", only rising, and full at
 * once when the run ends early; a greedy run's bar is indeterminate. The status is polled in the
 * background, so the poll never holds the bar itself. Once done, the proposal is labelled "Optimal"
 * or "Best found in N s" with its filled count and the places left empty, and accept and discard
 * work as today.
 */
import { useLocalSearchParams } from "expo-router";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { Button, EmptyState, InlineBanner, PageTitle, SkeletonRows, StatusChip, Txt, showToast } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { setProgress, startLoading } from "../../design/loading/loadingStore";
import { useTheme } from "../../design/theme/ThemeProvider";
import { previewsEnabled } from "../../config/previews";
import { DEFAULT_TIME_ZONE, formatNumericDate, formatNumericRange, formatTimeRange } from "../../locale";
import { HonestyLabel } from "./HonestyLabel";
import { filled, isActive, runProgress, type Job } from "./model";

const POLL_MS = 1000;
const TICK_MS = 250;
const TONE = { queued: "neutral", running: "info", succeeded: "success", failed: "danger" } as const;

/** Hold the bar while `job` runs and feed it elapsed over the limit. */
function useRunBar(job: Job | null): void {
  const { t } = useTranslation();
  const active = job !== null && isActive(job);
  const solver = job?.solver ?? null;
  const startedAt = job?.started_at ?? null;
  const limit = job?.time_limit_seconds ?? 0;
  const text = t("runs.up_to", { count: limit });
  useEffect(() => {
    if (!active || solver === null) return undefined;
    const done = startLoading();
    let floor = 0;
    const tick = () => {
      const value = runProgress({ status: "running", solver, started_at: startedAt, time_limit_seconds: limit }, Date.now(), floor);
      if (value !== null) floor = value;
      setProgress(value, value === null ? null : text);
    };
    tick();
    const timer = setInterval(tick, TICK_MS);
    return () => {
      clearInterval(timer);
      // Ended (or left): the bar completes at once and goes.
      if (solver !== "greedy") setProgress(1, text);
      done();
    };
  }, [active, solver, startedAt, limit, text]);
}

/** Read the run, and keep reading it in the background while it runs. */
function useJob(id: string | undefined): { job: Job | null; error: string | null; setJob: (job: Job) => void; reload: () => void } {
  const session = useSession();
  const [job, setJob] = useState<Job | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    if (!id) return undefined;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const read = async (background: boolean) => {
      try {
        const client = background ? session.quiet : session.api;
        const next = await call(client.GET("/api/v1/schedule-jobs/{id}", { params: { path: { id } } }));
        if (stopped) return;
        setJob(next);
        setError(null);
        if (isActive(next)) timer = setTimeout(() => void read(true), POLL_MS);
      } catch (failure) {
        if (!stopped) setError(codeOf(failure));
      }
    };
    void read(false);
    return () => {
      stopped = true;
      if (timer) clearTimeout(timer);
    };
  }, [id, session, attempt]);
  return { job, error, setJob, reload: () => setAttempt((n) => n + 1) };
}

function Proposal({ job }: { job: Job }) {
  const { t, i18n } = useTranslation();
  // A code this build does not know yet still reads as a reason.
  const reasonText = (code: string) => {
    const key = `runs.reasons.${code.replace(/^schedule\./, "")}`;
    return i18n.exists(key) ? t(key) : t("runs.reasons.other");
  };
  const { colors } = useTheme();
  const proposal = job.proposal;
  if (proposal === null) return null;
  const count = filled(proposal);
  const slots = new Map(proposal.slots.map((slot) => [slot.key, slot]));
  return (
    <View style={styles.section}>
      <View style={styles.summary}>
        <HonestyLabel job={job} />
        <Txt role="section" numeric testID="run-filled">
          {t("runs.filled", { filled: count.filled, required: count.required })}
        </Txt>
      </View>
      {proposal.unfilled.length === 0 ? (
        <Txt role="body" tone="secondary">
          {t("runs.all_filled")}
        </Txt>
      ) : (
        <View style={styles.list}>
          <Txt role="label" heading={2}>
            {t("runs.unfilled_title")}
          </Txt>
          {proposal.unfilled.map((gap) => {
            const slot = slots.get(gap.key);
            return (
              <View key={gap.key} testID="unfilled" style={[styles.row, { borderColor: colors.border.subtle }]}>
                {slot ? (
                  <Txt role="label" numeric style={styles.when}>
                    {`${formatNumericDate(slot.date)} ${formatTimeRange(slot.starts_at, slot.ends_at, DEFAULT_TIME_ZONE)}`}
                  </Txt>
                ) : null}
                <Txt role="meta" tone="warning">
                  {t("runs.places_empty", { count: gap.places })}
                </Txt>
                {gap.reasons.map((reason) => (
                  <Txt key={reason.code} role="meta" tone="secondary">
                    {reason.employees > 0 ? t("runs.reason", { reason: reasonText(reason.code), count: reason.employees }) : reasonText(reason.code)}
                  </Txt>
                ))}
              </View>
            );
          })}
        </View>
      )}
      {proposal.no_availability.length > 0 ? (
        <Txt role="meta" tone="secondary">
          {t("runs.no_availability", { count: proposal.no_availability.length })}
        </Txt>
      ) : null}
      {proposal.short_published.length > 0 ? (
        <Txt role="meta" tone="secondary">
          {t("runs.short_published", { count: proposal.short_published.length })}
        </Txt>
      ) : null}
      <Txt role="meta" tone="secondary" numeric>
        {t("runs.changes", { added: proposal.changes.added.length, removed: proposal.changes.removed.length })}
      </Txt>
    </View>
  );
}

export function RunScreen() {
  const { t } = useTranslation();
  const { id } = useLocalSearchParams<{ id: string }>();
  const session = useSession();
  const errorText = useErrorText();
  const { job, error, setJob, reload } = useJob(id);
  const [busy, setBusy] = useState<"accept" | "discard" | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  useRunBar(job);

  const accept = async () => {
    if (!id) return;
    setBusy("accept");
    setActionError(null);
    try {
      const answer = await call(session.api.POST("/api/v1/schedule-jobs/{id}/accept", { params: { path: { id } } }));
      showToast(t("runs.accepted", { count: answer.shifts_created + answer.shifts_updated }), { tone: "success" });
      reload();
    } catch (failure) {
      setActionError(codeOf(failure));
    } finally {
      setBusy(null);
    }
  };
  const discard = async () => {
    if (!id) return;
    setBusy("discard");
    setActionError(null);
    try {
      setJob(await call(session.api.POST("/api/v1/schedule-jobs/{id}/discard", { params: { path: { id } } })));
    } catch (failure) {
      setActionError(codeOf(failure));
    } finally {
      setBusy(null);
    }
  };

  const open = job?.status === "succeeded" && job.proposal_state === "open";
  return (
    <Screen testID="screen-run">
      <PageTitle eyebrow={[t("app.name"), t("nav.runs")]} title={job ? formatNumericRange(job.from, job.to) : t("nav.runs")}>
        {open ? (
          <>
            <Button variant="primary" icon="check" label={t("runs.accept")} busy={busy === "accept"} disabled={busy !== null} onPress={() => void accept()} testID="action-accept" />
            <Button label={t("runs.discard")} busy={busy === "discard"} disabled={busy !== null} onPress={() => void discard()} testID="action-discard" />
          </>
        ) : null}
      </PageTitle>
      {error ? (
        <InlineBanner tone="warning" title={errorText(error)} action={{ label: t("common.retry"), onPress: reload }} />
      ) : job === null ? (
        <SkeletonRows rows={4} />
      ) : (
        <View style={styles.section}>
          <View style={styles.summary}>
            <StatusChip status={TONE[job.status]} label={t(`runs.status.${job.status}`)} testID="run-status" />
            {job.status === "succeeded" && job.proposal_state !== "open" && job.proposal_state !== null ? (
              <Txt role="meta" tone="secondary" testID="run-proposal-state">
                {t(`runs.proposal.${job.proposal_state}`)}
              </Txt>
            ) : null}
          </View>
          {isActive(job) ? (
            <Txt role="body" tone="secondary" testID="run-progress">
              {job.solver === "greedy" ? t("runs.running") : t("runs.running_up_to", { count: job.time_limit_seconds })}
            </Txt>
          ) : null}
          {job.status === "failed" ? <InlineBanner tone="danger" title={t("runs.failed")} /> : null}
          {actionError ? <InlineBanner tone="warning" title={errorText(actionError)} /> : null}
          {job.status === "succeeded" ? <Proposal job={job} /> : null}
          {previewsEnabled && job.status === "succeeded" ? (
            <EmptyState
              testID="preview-proposal-diff"
              icon="layers"
              badge={t("preview.badge")}
              title={t("preview.proposal_diff_title")}
              body={t("preview.proposal_diff_body")}
              feature={t("preview.comes_with", { feature: "008" })}
            />
          ) : null}
        </View>
      )}
    </Screen>
  );
}

const styles = StyleSheet.create({
  section: { gap: 12 },
  summary: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 12 },
  list: { gap: 8 },
  row: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 12, padding: 12, borderWidth: 1 },
  when: { flexGrow: 1, minWidth: 160 },
});
