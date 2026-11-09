/**
 * Stop on a running or queued run (specs/008-auto-scheduler-review FR-028): it ends with its best
 * result so far, or cancelled when it has none yet.
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { Button, Txt, showToast } from "../../design/components";
import { type Job } from "./model";


export function RunStop({ job, onStopped }: { job: Job; onStopped: () => void }) {
  const { t } = useTranslation();
  const session = useSession();
  const errorText = useErrorText();
  const [busy, setBusy] = useState(false);
  const progress = job.progress;
  const hasResult = progress?.filled != null;
  const stop = async () => {
    setBusy(true);
    try {
      await call(session.api.POST("/api/v1/schedule-jobs/{id}/stop", { params: { path: { id: job.id } } }));
      onStopped();
    } catch (failure) {
      showToast(errorText(codeOf(failure)), { tone: "warning" });
    } finally {
      setBusy(false);
    }
  };
  return (
    <View style={styles.summary} testID="run-best-so-far">
      {hasResult ? (
        <Txt role="body" numeric testID="run-best">
          {t("runs.best_so_far", { filled: progress!.filled, needed: progress!.needed })}
        </Txt>
      ) : null}
      <Button
        variant="secondary"
        icon="close"
        label={job.stop_requested ? t("runs.stopping") : hasResult ? t("runs.stop_best") : t("runs.cancel_run")}
        busy={busy}
        disabled={busy || job.stop_requested}
        onPress={() => void stop()}
        testID="action-stop-run"
      />
    </View>
  );
}


const styles = StyleSheet.create({
  summary: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 12 },
});
