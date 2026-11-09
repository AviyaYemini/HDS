/**
 * One schedule run (specs/007-ux-redesign FR-042, FR-074, T079). While it runs, the loading bar is
 * determinate: elapsed time against the time limit, labelled "up to N s", only rising, and full at
 * once when the run ends early; a greedy run's bar is indeterminate. The status is polled in the
 * background, so the poll never holds the bar itself. Once done, the proposal is labelled "Optimal"
 * or "Best found in N s" with its filled count and the places left empty, and accept and discard
 * work as today.
 */
import { useLocalSearchParams } from "expo-router";
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { InlineBanner, LinkPressable, PageTitle, SkeletonRows, StatusChip, Txt } from "../../design/components";
import { Icon } from "../../design/icons/Icon";
import { webProps } from "../../design/web";
import { Screen } from "../../design/components/Screen";
import { endProgress, setProgress, startLoading } from "../../design/loading/loadingStore";
import { useTheme } from "../../design/theme/ThemeProvider";
import { DEFAULT_TIME_ZONE, formatNumericDate, formatNumericRange, formatTimeRange, startOfWeek } from "../../locale";
import { useLoad } from "../useLoad";
import { rosterHref } from "../roster/ProjectChooser";
import { HonestyLabel } from "./HonestyLabel";
import { ReviewWeek } from "./ReviewWeek";
import { numbersOf, reasonKey } from "./review";
import { STATUS_TONE, coveredText, elapsedNow, filled, isActive, projectsOf, runProgress, type Job, type Names } from "./model";

const POLL_MS = 1000;
const RETRY_MS = 3000;
const TICK_MS = 250;

/** A job and when it was read, by the device's clock. */
interface Reading {
  job: Job;
  readAt: number;
}

/** Hold the bar while the run goes and feed it elapsed (by the server's clock) over the limit. */
function useRunBar(reading: Reading | null): void {
  const { t } = useTranslation();
  const latest = useRef(reading);
  useEffect(() => {
    latest.current = reading;
  });
  const job = reading?.job ?? null;
  const active = job !== null && isActive(job);
  const solver = job?.solver ?? null;
  const limit = job?.time_limit_seconds ?? 0;
  const text = t("runs.up_to", { count: limit });
  useEffect(() => {
    if (!active || solver === null) return undefined;
    const done = startLoading();
    let floor = 0;
    const tick = () => {
      const now = latest.current;
      const elapsed = now === null ? null : elapsedNow(now.job, now.readAt, Date.now());
      const value = runProgress({ status: "running", solver, time_limit_seconds: limit }, elapsed, floor);
      if (value !== null) floor = value;
      setProgress(value, value === null ? null : text);
    };
    tick();
    const timer = setInterval(tick, TICK_MS);
    return () => {
      clearInterval(timer);
      // Ended (or left): the bar completes at once and goes, and nothing after inherits it.
      endProgress();
      done();
    };
  }, [active, solver, limit, text]);
}

/**
 * Read the run, and keep reading it in the background while it runs. A failed background read keeps
 * the last answer on screen, says so, and tries again; only a failed first read replaces the page.
 */
function useJob(id: string | undefined) {
  const session = useSession();
  const [reading, setReading] = useState<Reading | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pollError, setPollError] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    if (!id) return undefined;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let seen = false;
    const read = async (background: boolean) => {
      try {
        const client = background ? session.quiet : session.api;
        const next = await call(client.GET("/api/v1/schedule-jobs/{id}", { params: { path: { id } } }));
        if (stopped) return;
        seen = true;
        setReading({ job: next, readAt: Date.now() });
        setError(null);
        setPollError(null);
        if (isActive(next)) timer = setTimeout(() => void read(true), POLL_MS);
      } catch (failure) {
        if (stopped) return;
        if (!seen) {
          setError(codeOf(failure));
          return;
        }
        setPollError(codeOf(failure));
        timer = setTimeout(() => void read(true), RETRY_MS);
      }
    };
    void read(false);
    return () => {
      stopped = true;
      if (timer) clearTimeout(timer);
    };
  }, [id, session, attempt]);
  return {
    reading,
    error,
    pollError,
    setJob: (job: Job) => setReading({ job, readAt: Date.now() }),
    reload: () => setAttempt((n) => n + 1),
  };
}

/**
 * Project, shift type and people's names, to read the proposal by (FR-080). Everyone, deactivated
 * people included, so an older run still names who it proposed.
 */
function useNames(): Names & { staff: { id: string; full_name: string }[] } {
  const session = useSession();
  const loaded = useLoad(async () => {
    const [projects, types, people] = await Promise.all([
      call(session.api.GET("/api/v1/projects")),
      call(session.api.GET("/api/v1/shift-types")),
      call(session.api.GET("/api/v1/employees")),
    ]);
    return {
      projects: new Map(projects.items.map((p) => [p.id, p.name])),
      types: new Map(types.items.map((type) => [type.id, type.name])),
      people: new Map(people.items.map((person) => [person.id, person.full_name])),
      // Who a proposal may place: active staff, never a client account (008 FR-008).
      staff: people.items.filter((person) => person.role !== "client" && person.is_active).map((person) => ({ id: person.id, full_name: person.full_name })),
    };
  });
  return loaded.data ?? { projects: new Map(), types: new Map(), people: new Map(), staff: [] };
}

function Proposal({ job, names }: { job: Job; names: Names }) {
  const { t, i18n } = useTranslation();
  // A code this build does not know yet still reads as a reason.
  const reasonText = (code: string) => t(reasonKey(code, (key) => i18n.exists(key)));
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
                  <View style={styles.when}>
                    <Txt role="label">{[names.projects.get(slot.project_id), names.types.get(slot.shift_type_id)].filter(Boolean).join(" · ")}</Txt>
                    <Txt role="meta" tone="secondary" numeric>
                      {`${formatNumericDate(slot.date)} ${formatTimeRange(slot.starts_at, slot.ends_at, DEFAULT_TIME_ZONE)}`}
                    </Txt>
                  </View>
                ) : null}
                <Txt role="meta" tone="warning">
                  {t("runs.places_empty", { count: gap.places })}
                </Txt>
                {gap.reasons.map((reason) => (
                  <View key={reason.code} testID="unfilled-reason" {...webProps({ dataSet: { code: reason.code } })}>
                    <Txt role="meta" tone="secondary">
                      {reason.employees > 0 ? t("runs.reason", { reason: reasonText(reason.code), count: reason.employees }) : reasonText(reason.code)}
                    </Txt>
                    {/* Rest and the weekly maximum name each person, with the numbers (008 FR-013). */}
                    {(reason.people ?? []).map((person) => {
                      const numbers = numbersOf(person, i18n.language);
                      const who = names.people.get(person.user_id) ?? t("runs.unknown_person");
                      return (
                        <Txt key={person.user_id} role="meta" tone="secondary" numeric testID="unfilled-person" {...webProps({ dataSet: { userId: person.user_id } })}>
                          {numbers ? t("runs.person_numbers", { person: who, numbers: t(numbers.key, numbers.values) }) : who}
                        </Txt>
                      );
                    })}
                  </View>
                ))}
              </View>
            );
          })}
        </View>
      )}
      {proposal.no_availability.length > 0 ? (
        <View testID="no-availability" style={styles.list}>
          <Txt role="label" heading={2}>
            {t("runs.no_availability", { count: proposal.no_availability.length })}
          </Txt>
          <Txt role="body" tone="secondary">
            {proposal.no_availability.map((id) => names.people.get(id) ?? t("runs.unknown_person")).join(", ")}
          </Txt>
        </View>
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
  const errorText = useErrorText();
  const { colors } = useTheme();
  const { reading, error, pollError, reload } = useJob(id);
  const job = reading?.job ?? null;
  const names = useNames();
  useRunBar(reading);

  return (
    <Screen testID="screen-run">
      <PageTitle eyebrow={[t("app.name"), t("nav.runs")]} title={job ? formatNumericRange(job.from, job.to) : t("nav.runs")} />
      {error ? (
        <InlineBanner tone="warning" title={errorText(error)} action={{ label: t("common.retry"), onPress: reload }} />
      ) : job === null ? (
        <SkeletonRows rows={4} />
      ) : (
        <View style={styles.section}>
          <Txt role="body" testID="run-projects-covered">
            {t("runs.covers", { projects: coveredText(job.project_ids, names.projects, t("runs.every_project")) })}
          </Txt>
          <View style={styles.summary}>
            <StatusChip status={STATUS_TONE[job.status]} label={t(`runs.status.${job.status}`)} testID="run-status" />
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
          {pollError ? <InlineBanner tone="warning" testID="banner-run-poll" title={t("runs.poll_failed")} /> : null}
          {job.status === "succeeded" ? <Proposal job={job} names={names} /> : null}
          {job.status === "succeeded" && (job.proposal_state === "accepted" || job.proposal_state === "partly_accepted") ? (
            <View style={styles.summary}>
              {projectsOf(job).map((project) => (
                <LinkPressable
                  key={project}
                  href={rosterHref(project, startOfWeek(job.from))}
                  testID="action-open-in-roster"
                  {...webProps({ dataSet: { projectId: project } })}
                  style={[styles.link, { borderColor: colors.accent.default }]}
                >
                  <Icon name="grid" size={18} color={colors.text.primary} />
                  <Txt role="label">{t("runs.open_in_roster", { project: names.projects.get(project) ?? "" })}</Txt>
                </LinkPressable>
              ))}
            </View>
          ) : null}
          {job.status === "succeeded" ? (
            // Run again: the form on this run's range, projects, controls and chips, starting from
            // its proposal and keeping its locks (FR-019, FR-021a).
            <LinkPressable href={`/runs?again=${job.id}`} testID="action-run-again" style={[styles.link, { borderColor: colors.accent.default }]}>
              <Icon name="play" size={18} color={colors.text.primary} />
              <Txt role="label">{t("runs.run_again")}</Txt>
            </LinkPressable>
          ) : null}
          {job.status === "succeeded" ? <ReviewWeek job={job} names={names} people={names.staff} onDecided={reload} /> : null}
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
  link: { flexDirection: "row", alignItems: "center", gap: 8, minHeight: 44, paddingHorizontal: 12, borderWidth: 1 },
  when: { flexGrow: 1, minWidth: 160 },
});
