/**
 * One schedule run in the list (specs/007-ux-redesign FR-042): its range, status and how good its
 * proposal is, opening the run.
 */
import { useTranslation } from "react-i18next";
import { StyleSheet } from "react-native";

import { LinkPressable, StatusChip, Txt } from "../../design/components";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { formatNumericRange } from "../../locale";
import { HonestyLabel } from "./HonestyLabel";
import { STATUS_TONE, coveredText, type JobSummary } from "./model";


export function RunRow({ job, names }: { job: JobSummary; names: Map<string, string> }) {
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

const styles = StyleSheet.create({
  row: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 12, padding: 12, borderWidth: 1 },
  range: { flexGrow: 1, minWidth: 160 },
});
