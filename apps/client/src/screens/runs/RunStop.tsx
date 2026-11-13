/**
 * Stop on a running or queued run (specs/008-auto-scheduler-review FR-028): it ends with its best
 * result so far, or cancelled when it has none yet.
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { Button, showToast } from "../../design/components";
import { useTheme } from "../../design/theme/ThemeProvider";
import { KeyFigures } from "../deck/parts";
import { type Job } from "./model";
import { shared } from "./runStyles";


export function RunStop({ job, onStopped }: { job: Job; onStopped: () => void }) {
  const { t } = useTranslation();
  const session = useSession();
  const errorText = useErrorText();
  const { colors } = useTheme();
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
  const filled = progress?.filled ?? 0;
  const needed = progress?.needed ?? 0;
  const share = needed > 0 ? Math.min(100, (filled / needed) * 100) : 0;
  return (
    <View style={styles.root} testID="run-best-so-far">
      {hasResult ? (
        // The live count as the finished page's figure 01, with a bar, so the jump is calm (the owner, 2026-11-13).
        <>
          <KeyFigures
            figures={[
              {
                key: "filled",
                label: t("runsum.figure.filled_so_far"),
                value: `${filled}/${needed}`,
                valueTestID: "run-best",
                detail: t("runs.best_so_far", { filled, needed }),
              },
            ]}
          />
          <View style={[styles.track, { backgroundColor: colors.bg.selected }]} accessibilityLabel={t("runs.best_so_far", { filled, needed })} testID="run-best-bar">
            <View style={[styles.fill, { width: `${share}%`, backgroundColor: colors.text.primary }]} />
          </View>
        </>
      ) : null}
      <View style={shared.summary}>
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
    </View>
  );
}

const styles = StyleSheet.create({
  root: { gap: 12 },
  track: { height: 8 },
  fill: { height: 8 },
});

