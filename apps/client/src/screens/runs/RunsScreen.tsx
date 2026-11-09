/**
 * Schedule runs (specs/007-ux-redesign FR-042, T079): the organisation's runs, newest first, each with
 * its range, status and how good its proposal is; and the generate form, a range that cannot start in
 * the past chosen from the planning presets or the calendar. Generating opens the run.
 */
import { useLocalSearchParams, useRouter } from "expo-router";
import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { useCapabilities } from "../../capabilities";
import {
  Button,
  DateRangeField,
  EmptyState,
  InlineBanner,
  LinkPressable,
  PageTitle,
  SearchablePicker,
  SkeletonRows,
  StatusChip,
  Txt,
} from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { DEFAULT_TIME_ZONE, addDays, compareDates, formatNumericRange, presetRange, todayIn, type DateRange } from "../../locale";
import { useLoad } from "../useLoad";
import { ChipEditor } from "./ChipEditor";
import { HonestyLabel } from "./HonestyLabel";
import { STATUS_TONE, coveredText, digest, isActive, typesOf, type JobSummary } from "./model";
import { DEFAULT_CONTROLS, type Chip, type Controls } from "./steering";

/** A run to run again (FR-019, FR-021a): its range, projects, controls and chips, and its id. */
interface Again {
  id: string;
  from: string;
  to: string;
  project_ids: string[];
  controls: Controls;
  chips: Chip[];
}

/** A run covers at most this many days (the API's limit, 004 FR-005). */
const MAX_DAYS = 31;
const REFRESH_MS = 2000;


function RunRow({ job, names }: { job: JobSummary; names: Map<string, string> }) {
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
      <Txt role="meta" tone="secondary" testID="run-row-projects" style={styles.range}>
        {coveredText(job.project_ids, names, t("runs.every_project"))}
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

/** A project the generate form may offer: its name and type (training and security don't mix). */
interface Offered {
  id: string;
  name: string;
  type: string;
}

/**
 * The projects a run may cover, for the reader (FR-081): a Manager's own active projects, an
 * Admin's every active one, as the API scopes a run (T108); and every project's name, for the rows.
 */
function useRunProjects(): { offered: Offered[] | null; names: Map<string, string>; error: string | null; reload: () => Promise<void> } {
  const session = useSession();
  const capabilities = useCapabilities();
  const projects = useLoad(async () => (await call(session.api.GET("/api/v1/projects"))).items, []);
  const all = projects.data ?? [];
  const offered = projects.data
    ? all
        .filter((p) => p.status === "active" && capabilities.hasOnProject("roster.edit:project", p.id))
        .map((p) => ({ id: p.id, name: p.name, type: p.type }))
    : null;
  return { offered, names: new Map(all.map((p) => [p.id, p.name])), error: projects.error, reload: projects.reload };
}

/**
 * The roster's week as a run's range, never starting before today, as the date field allows: this
 * week from today on, or next week when the whole week has passed (FR-032).
 */
export function fromPreset(week: string, today: string): DateRange {
  const end = addDays(week, 6);
  if (compareDates(end, today) < 0) return presetRange("next-week", today);
  return { start: compareDates(week, today) < 0 ? today : week, end };
}

function GenerateForm({ onCancel, offered, again, preset }: { onCancel: () => void; offered: Offered[]; again?: Again | null; preset?: { week: string; project: string | null } | null }) {
  const { t } = useTranslation();
  const session = useSession();
  const router = useRouter();
  const errorText = useErrorText();
  const { colors, isDesktop } = useTheme();
  const today = todayIn(DEFAULT_TIME_ZONE);
  const [range, setRange] = useState<DateRange>(() =>
    again ? { start: again.from, end: again.to } : preset ? fromPreset(preset.week, today) : presetRange("next-week", today),
  );
  // The two plain controls and the chips (FR-017, FR-020); a rerun starts with the earlier run's.
  const [controls, setControls] = useState<Controls>(() => again?.controls ?? DEFAULT_CONTROLS);
  const [chips, setChips] = useState<Chip[]>(() => again?.chips ?? []);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Every offered project at first when they are all of one type (every Manager today); with
  // both types offered, none, and a quick choice of each type: the default never mixes training
  // and security, though choosing both on purpose stays allowed (reviewer's ruling, 2026-11-08,
  // after the owner's note in spec.md's assumptions).
  const types = typesOf(offered);
  // From the roster's "Generate this week": its project, when the person may run it (FR-032).
  const [picked, setPicked] = useState<string[] | null>(() =>
    again ? again.project_ids : preset?.project && offered.some((p) => p.id === preset.project) ? [preset.project] : null,
  );
  const projects = picked ?? (types.length > 1 ? [] : offered.map((p) => p.id));
  // The key follows the request itself: a repeat of the same one (a double press, a retry after a
  // lost answer) answers the same run, and any change of range or projects (the offered list
  // arriving late included) is a new request, never a reused key (group O review).
  const [form] = useState(() => `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`);
  const key = `run-${form}-${digest(`${range.start}|${range.end}|${projects.join(",")}|${JSON.stringify(controls)}|${JSON.stringify(chips)}|${again?.id ?? ""}`)}`;
  const summary =
    projects.length === offered.length
      ? t("runs.projects_all", { count: offered.length })
      : projects.length === 0
        ? t("runs.projects_none")
        : offered.filter((p) => projects.includes(p.id)).map((p) => p.name).join(", ");
  const noProjects = projects.length === 0;

  const tooLong = compareDates(range.end, addDays(range.start, MAX_DAYS - 1)) > 0;

  const generate = async () => {
    setBusy(true);
    setError(null);
    try {
      const job = await call(
        session.api.POST("/api/v1/schedule-jobs", {
          body: { from: range.start, to: range.end, project_ids: projects, solver: "cpsat", controls, chips, ...(again ? { based_on: again.id } : {}) },
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
    // Unboxed on a phone, so the 308-pixel calendar fits the column (FR-079).
    <View testID="run-form" style={[styles.form, isDesktop ? null : styles.formPhone, { borderColor: colors.border.subtle, backgroundColor: colors.bg.surface }]}>
      <SearchablePicker
        label={t("runs.projects")}
        items={offered.map((p) => ({ id: p.id, label: p.name, secondary: t(`project.type.${p.type}`) }))}
        value={null}
        onChange={() => undefined}
        multiple={{ values: projects, onChange: setPicked, summary }}
        testID="run-projects"
      />
      {types.length > 1 ? (
        <View style={styles.formActions}>
          {types.map((type) => (
            <Button
              key={type}
              label={t(`runs.projects_of_type.${type}`)}
              onPress={() => setPicked(offered.filter((p) => p.type === type).map((p) => p.id))}
              testID={`action-projects-${type}`}
            />
          ))}
        </View>
      ) : null}
      <DateRangeField label={t("runs.range")} value={range} onChange={setRange} forward minDate={today} />
      <Txt role="meta" tone="secondary">
        {t("runs.range_hint")}
      </Txt>
      {again ? (
        <Txt role="meta" tone="secondary" testID="run-again-note">
          {t("runs.again_note")}
        </Txt>
      ) : null}
      <ChipEditor controls={controls} onControls={setControls} chips={chips} onChips={setChips} projects={offered.filter((p) => projects.includes(p.id))} />
      {error ? <InlineBanner tone="warning" title={errorText(error)} /> : null}
      <View style={styles.formActions}>
        <Button variant="primary" icon="play" label={t("runs.generate")} onPress={() => void generate()} busy={busy} disabled={tooLong || noProjects} disabledReason={tooLong ? t("schedule.range_invalid") : noProjects ? t("runs.projects_none") : undefined} testID="action-generate" />
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
  // A failed next page keeps the list on screen and says so beside the button (R118).
  const [moreError, setMoreError] = useState<string | null>(null);

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
    setMoreError(null);
    try {
      const page = await call(session.api.GET("/api/v1/schedule-jobs", { params: { query: { limit: 50, cursor } } }));
      setItems((held) => {
        const ids = new Set((held ?? []).map((job) => job.id));
        return [...(held ?? []), ...page.items.filter((job) => !ids.has(job.id))];
      });
      setCursor(page.next_cursor);
    } catch (failure) {
      setMoreError(codeOf(failure));
    } finally {
      setLoadingMore(false);
    }
  };

  return { data: items, error, reload: () => first(false), more: cursor === null ? null : more, loadingMore, moreError };
}

export function RunsScreen() {
  const { t } = useTranslation();
  const errorText = useErrorText();
  // "Run again" opens the form on an earlier run (?again=<id>, FR-019).
  const params = useLocalSearchParams<{ again?: string; week?: string; project?: string }>();
  // The roster's "Generate this week" opens the form on that week and project (FR-032).
  const [creating, setCreating] = useState(!!params.again || !!params.week);
  const preset = params.week ? { week: params.week, project: params.project ?? null } : null;
  const jobs = useRuns();
  const projects = useRunProjects();
  const session = useSession();
  const again = useLoad(async () => {
    if (!params.again) return null;
    const job = await call(session.api.GET("/api/v1/schedule-jobs/{id}", { params: { path: { id: params.again } } }));
    return { id: job.id, from: job.from, to: job.to, project_ids: job.project_ids, controls: job.controls, chips: job.chips } satisfies Again;
  }, [params.again]);
  return (
    <Screen testID="screen-runs">
      <PageTitle eyebrow={[t("app.name"), t("nav.group.manage")]} title={t("nav.runs")}>
        {creating ? null : <Button variant="primary" icon="plus" label={t("runs.new")} onPress={() => setCreating(true)} testID="action-new-run" />}
      </PageTitle>
      {!creating ? null : params.again && again.error ? (
        // The earlier run could not be read: say so, never a plain form that drops its chips (R51).
        <InlineBanner tone="warning" testID="banner-run-again" title={errorText(again.error)} action={{ label: t("common.retry"), onPress: () => void again.reload() }} />
      ) : projects.error ? (
        // A failed read of the projects says so, with a way to try again (CodeRabbit, PR 20).
        <InlineBanner tone="warning" title={errorText(projects.error)} action={{ label: t("common.retry"), onPress: () => void projects.reload() }} />
      ) : projects.offered === null || (params.again && again.data === null && !again.error) ? (
        <SkeletonRows rows={2} />
      ) : projects.offered.length === 0 ? (
        <EmptyState icon="briefcase" title={t("runs.no_projects_title")} body={t("runs.no_projects_body")} testID="run-no-projects" />
      ) : (
        <GenerateForm onCancel={() => setCreating(false)} offered={projects.offered} again={again.data} preset={preset} />
      )}
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
            <RunRow key={job.id} job={job} names={projects.names} />
          ))}
          {jobs.more ? <Button label={t("runs.more")} onPress={() => void jobs.more?.()} busy={jobs.loadingMore} testID="action-more-runs" /> : null}
          {jobs.moreError ? <InlineBanner tone="warning" testID="banner-more-runs" title={errorText(jobs.moreError)} /> : null}
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
  formPhone: { padding: 0, borderWidth: 0 },
  formActions: { flexDirection: "row", flexWrap: "wrap", gap: 8 },
});
