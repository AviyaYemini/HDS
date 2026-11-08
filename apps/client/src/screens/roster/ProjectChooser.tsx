/**
 * The roster without a project (CodeRabbit, PR 18; R113): the projects to open, the ones the reader
 * edits first, each opening its week; the others open read-only. Someone who edits exactly one
 * project goes straight to it.
 */
import { useRouter } from "expo-router";
import { useEffect } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { useCapabilities } from "../../capabilities";
import { EmptyState, InlineBanner, LinkPressable, SkeletonRows, Txt } from "../../design/components";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { useLoad } from "../useLoad";

/** The address of a project's roster, keeping the week when one is chosen. */
export function rosterHref(project: string, week?: string): string {
  return week ? `/roster?project=${project}&week=${week}` : `/roster?project=${project}`;
}

/** `stay`: list every project even when the reader edits only one (`/roster?all=1`). */
export function ProjectChooser({ week, stay = false }: { week?: string; stay?: boolean }) {
  const { t } = useTranslation();
  const session = useSession();
  const router = useRouter();
  const capabilities = useCapabilities();
  const errorText = useErrorText();
  const { colors } = useTheme();
  const projects = useLoad(async () => (await call(session.api.GET("/api/v1/projects"))).items, []);
  // Split at render: the capabilities are read afresh each time, so they are no load's dependency.
  const mine = (projects.data ?? []).filter((p) => capabilities.hasOnProject("roster.edit:project", p.id));
  const others = (projects.data ?? []).filter((p) => !capabilities.hasOnProject("roster.edit:project", p.id));
  const only = !stay && mine.length === 1 ? mine[0]!.id : null;
  useEffect(() => {
    if (only) router.replace(rosterHref(only, week) as never);
  }, [only, week, router]);

  if (projects.error) {
    return <InlineBanner tone="warning" title={errorText(projects.error)} action={{ label: t("common.retry"), onPress: () => void projects.reload() }} />;
  }
  if (projects.data === null || only) return <SkeletonRows rows={4} />;
  if (mine.length + others.length === 0) {
    return <EmptyState icon="calendar" title={t("roster.no_projects")} body={t("roster.no_projects_body")} />;
  }
  const row = (project: { id: string; name: string }, editable: boolean) => (
    <LinkPressable
      key={project.id}
      href={rosterHref(project.id, week)}
      testID="roster-project"
      {...webProps({ dataSet: { projectId: project.id, editable: String(editable) } })}
      style={[styles.row, { borderColor: colors.border.subtle }]}
    >
      <Txt role="label" style={styles.name}>
        {project.name}
      </Txt>
      {editable ? null : (
        <Txt role="meta" tone="secondary">
          {t("roster.read_only")}
        </Txt>
      )}
    </LinkPressable>
  );
  return (
    <View style={styles.list} testID="roster-projects">
      <Txt role="section" heading={2}>
        {t("roster.choose_project")}
      </Txt>
      {mine.map((p) => row(p, true))}
      {others.map((p) => row(p, false))}
    </View>
  );
}

const styles = StyleSheet.create({
  list: { gap: 8 },
  row: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 12, padding: 12, borderWidth: 1 },
  name: { flexGrow: 1, flexShrink: 1, flexBasis: 160 },
});
