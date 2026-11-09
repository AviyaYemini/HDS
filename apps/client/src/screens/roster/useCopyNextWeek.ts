/**
 * "Copy to next week" after a publish (specs/008-auto-scheduler-review FR-033): a dry run first; when
 * the copy would replace drafts next week already holds, the manager is asked before any is
 * replaced. Then it opens next week.
 */
import { useRouter } from "expo-router";
import { useState } from "react";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { showToast } from "../../design/components";
import { addDays } from "../../locale";
import { rosterHref } from "./ProjectChooser";

export function useCopyNextWeek(project: string | null, week: string) {
  const session = useSession();
  const router = useRouter();
  const errorText = useErrorText();
  /** How many of next week's drafts a copy would replace, while the manager is asked (FR-033). */
  const [copyReplaces, setCopyReplaces] = useState<number | null>(null);
  const copyToNextWeek = async (confirmed = false) => {
    if (!project) return;
    const copy = (dry_run: boolean) =>
      call(
        session.api.POST("/api/v1/projects/{id}/copy-week", {
          params: { path: { id: project } },
          body: { from_week: week, to_week: addDays(week, 7), on_conflict: "overwrite", dry_run },
        }),
      );
    try {
      if (!confirmed) {
        const plan = await copy(true);
        const replaced = plan.changes.filter((c) => c.action === "replace").length;
        if (replaced > 0) {
          setCopyReplaces(replaced);
          return;
        }
      }
      setCopyReplaces(null);
      await copy(false);
      router.push(rosterHref(project, addDays(week, 7)) as never);
    } catch (failure) {
      showToast(errorText(codeOf(failure)), { tone: "warning" });
    }
  };
  return { copyReplaces, setCopyReplaces, copyToNextWeek };
}
