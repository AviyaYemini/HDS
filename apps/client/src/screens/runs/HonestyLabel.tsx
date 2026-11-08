/** How good a proposal is, as every run shows it: "Optimal" or "Best found in N s" (FR-042). */
import { useTranslation } from "react-i18next";

import { StatusChip } from "../../design/components";
import { honesty, type JobSummary } from "./model";

export function HonestyLabel({ job, testID = "run-honesty-label" }: { job: JobSummary; testID?: string }) {
  const { t } = useTranslation();
  const label = honesty(job);
  if (label === null) return null;
  return label.kind === "optimal" ? (
    <StatusChip status="success" label={t("runs.optimal")} testID={testID} />
  ) : (
    <StatusChip status="info" label={t("runs.best_found", { count: label.seconds })} testID={testID} />
  );
}
