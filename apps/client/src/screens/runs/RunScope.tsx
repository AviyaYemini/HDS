/**
 * A run's scope on one line (the owner, 2026-11-13): "32 security projects · 15/11–21/11", or the
 * names when there are one or two, with the full list behind "show names". The same while a run
 * works and once it is done, so the page does not jump when the result arrives.
 */
import { useTranslation } from "react-i18next";

import { call } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { Txt } from "../../design/components";
import { formatNumericRange } from "../../locale";
import { Disclosure } from "../deck/parts";
import { useLoad } from "../useLoad";
import { coveredText, type Job, type Names } from "./model";
import { scopeOf } from "./runSummary";

export function RunScope({ job, names }: { job: Pick<Job, "project_ids" | "from" | "to">; names: Names }) {
  const { t } = useTranslation();
  const { api } = useSession();
  const projects = useLoad(async () => (await call(api.GET("/api/v1/projects"))).items, []);
  const scope = scopeOf(job.project_ids ?? [], (projects.data ?? []).map((p) => ({ id: p.id, name: p.name, type: p.type })));
  const range = formatNumericRange(job.from, job.to);
  const summary = scope.all
    ? t("runsum.scope.all", { range })
    : scope.names
      ? t("runsum.scope.named", { names: scope.names.join(", "), range })
      : t(`runsum.scope.${scope.type}`, { count: scope.count, range });
  return (
    <Disclosure testID="run-projects-covered" summary={summary} showLabel={t("runsum.show_names")} hideLabel={t("runsum.hide")}>
      <Txt role="meta" tone="secondary">
        {coveredText(job.project_ids ?? [], names.projects, t("runs.every_project"))}
      </Txt>
    </Disclosure>
  );
}
