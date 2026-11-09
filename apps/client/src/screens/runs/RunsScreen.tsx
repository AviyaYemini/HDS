/**
 * Schedule runs (specs/007-ux-redesign FR-042, T079): the organisation's runs, newest first, each with
 * its range, status and how good its proposal is; and the generate form, a range that cannot start in
 * the past chosen from the planning presets or the calendar. Generating opens the run.
 */
import { useLocalSearchParams } from "expo-router";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { useErrorText } from "../../api/errors";
import { Button, EmptyState, InlineBanner, PageTitle, SkeletonRows } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { GenerateForm } from "./GenerateForm";
import { RunRow } from "./RunRow";
import { useAgain, useRunProjects, useRuns } from "./useRuns";

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
  const again = useAgain(params.again);
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
});
