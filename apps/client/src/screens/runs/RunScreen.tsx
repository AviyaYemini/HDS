/**
 * One schedule run (specs/007-ux-redesign FR-042, FR-074, T079). While it runs, the loading bar is
 * determinate: elapsed time against the time limit, labelled "up to N s", only rising, and full at
 * once when the run ends early; a greedy run's bar is indeterminate. The status is polled in the
 * background, so the poll never holds the bar itself. Once done, the proposal is labelled "Optimal"
 * or "Best found in N s" with its filled count and the places left empty, and accept and discard
 * work as today.
 */
import { useLocalSearchParams } from "expo-router";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { useErrorText } from "../../api/errors";
import { InlineBanner, LinkPressable, PageTitle, SkeletonRows, StatusChip, Txt } from "../../design/components";
import { Icon } from "../../design/icons/Icon";
import { webProps } from "../../design/web";
import { Screen } from "../../design/components/Screen";
import { useTheme } from "../../design/theme/ThemeProvider";
import { formatNumericRange, startOfWeek } from "../../locale";
import { rosterHref } from "../roster/ProjectChooser";
import { RecordPanel } from "./RecordPanel";
import { ReviewWeek } from "./ReviewWeek";
import { STATUS_TONE, isActive, projectsOf } from "./model";
import { Proposal } from "./RunProposal";
import { RunScope } from "./RunScope";
import { RunStop } from "./RunStop";
import { useJob, useNames, useRunBar } from "./useRun";
import { shared } from "./runStyles";

export function RunScreen() {
  const { t } = useTranslation();
  const { id } = useLocalSearchParams<{ id: string }>();
  const errorText = useErrorText();
  const { colors } = useTheme();
  const { reading, error, pollError, reload } = useJob(id);
  const job = reading?.job ?? null;
  const names = useNames();
  useRunBar(reading);
  // The record reads again whenever the run moves: its state, a Stop, a decision or an edit (FR-030).
  const [version, setVersion] = useState(-1);
  const stamp = job ? `${job.status}:${job.ended ?? ""}:${String(job.stop_requested)}:${job.proposal_state ?? ""}:${version}` : "";

  return (
    // No scroll view of its own: the shell's is the one that scrolls, so the review's action bar can
    // stick to the bottom of a phone's screen (the owner, 2026-11-13).
    <Screen testID="screen-run" fixed>
      <PageTitle eyebrow={[t("app.name"), t("nav.runs")]} title={job ? formatNumericRange(job.from, job.to) : t("nav.runs")} />
      {error ? (
        <InlineBanner tone="warning" title={errorText(error)} action={{ label: t("common.retry"), onPress: reload }} />
      ) : job === null ? (
        <SkeletonRows rows={4} />
      ) : (
        <View style={styles.section}>
          {/* While it works, the same one-line scope as the finished page (the owner, 2026-11-13). */}
          {job.status !== "succeeded" ? <RunScope job={job} names={names} /> : null}
          <View style={shared.summary}>
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
          {isActive(job) ? <RunStop job={job} onStopped={reload} /> : null}
          {job.ended === "stopped" ? (
            // A run stopped by a person keeps its best result, said in words (FR-028).
            <Txt role="meta" tone="secondary" testID="run-stopped-early">
              {t("runs.stopped_early")}
            </Txt>
          ) : null}
          {job.status === "failed" ? <InlineBanner tone="danger" title={t("runs.failed")} /> : null}
          {pollError ? <InlineBanner tone="warning" testID="banner-run-poll" title={t("runs.poll_failed")} /> : null}
          {job.status === "succeeded" ? <ReviewWeek job={job} names={names} people={names.staff} onDecided={reload} onVersion={setVersion} /> : null}
          {/* Below the week: the places left empty, with the run's reasons (coverage, not warnings). */}
          {job.status === "succeeded" ? <Proposal job={job} names={names} /> : null}
          {job.status === "succeeded" && (job.proposal_state === "accepted" || job.proposal_state === "partly_accepted") ? (
            <View style={shared.summary}>
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
          {/* Every run's record, once it has one (FR-030). */}
          <RecordPanel jobId={job.id} stamp={stamp} name={(person) => names.people.get(person) ?? t("runs.unknown_person")} />
        </View>
      )}
    </Screen>
  );
}


const styles = StyleSheet.create({
  section: { gap: 12 },
  link: { flexDirection: "row", alignItems: "center", gap: 8, minHeight: 44, paddingHorizontal: 12, borderWidth: 1 },
});
