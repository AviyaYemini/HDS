/**
 * A run's record (specs/008-auto-scheduler-review FR-030, FR-031): who ran it and with what, how it
 * ended, each edit, override, accept and discard, each with who and when, oldest first, read a
 * page at a time. It only grows; nothing here changes it.
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import type { components } from "@hds/api-client";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { Button, InlineBanner, SkeletonRows, Txt } from "../../design/components";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { DEFAULT_TIME_ZONE, dateOf, formatNumericDate, formatTime } from "../../locale";
import { useLoad } from "../useLoad";

type Step = components["schemas"]["RecordItem"];

const PAGE = 50;

/** The words for one step's detail, when it has one worth saying. */
function detail(step: Step, t: (key: string, values?: Record<string, unknown>) => string): string | null {
  const body = step.body as Record<string, unknown>;
  if (step.kind === "overridden" && typeof body.reason === "string") return t("runs.record_detail.reason", { reason: body.reason });
  if (step.kind === "edited" && body.op && typeof body.op === "object") return t(`runs.record_detail.op.${String((body.op as { op?: string }).op ?? "edit")}`);
  if (step.kind === "finished" && typeof body.filled === "number") return t("runs.record_detail.filled", { filled: body.filled, needed: body.needed ?? 0 });
  return null;
}

export function RecordPanel({ jobId, name }: { jobId: string; name: (id: string) => string }) {
  const { t } = useTranslation();
  const { colors } = useTheme();
  const session = useSession();
  const errorText = useErrorText();
  const first = useLoad(
    async () => call(session.api.GET("/api/v1/schedule-jobs/{id}/record", { params: { path: { id: jobId }, query: { limit: PAGE } } })),
    [jobId],
  );
  const [more, setMore] = useState<Step[]>([]);
  const [after, setAfter] = useState<number | null | undefined>(undefined);
  const [busy, setBusy] = useState(false);
  const [problem, setProblem] = useState<string | null>(null);

  if (first.error && !first.data) return <InlineBanner tone="warning" title={errorText(first.error)} action={{ label: t("common.retry"), onPress: first.reload }} />;
  if (!first.data) return <SkeletonRows rows={3} />;
  const steps = [...first.data.items, ...more];
  const next = after === undefined ? first.data.next_after : after;

  const loadMore = async () => {
    if (next == null) return;
    setBusy(true);
    setProblem(null);
    try {
      const page = await call(session.api.GET("/api/v1/schedule-jobs/{id}/record", { params: { path: { id: jobId }, query: { after: next, limit: PAGE } } }));
      setMore((now) => [...now, ...page.items]);
      setAfter(page.next_after);
    } catch (failure) {
      setProblem(codeOf(failure));
    } finally {
      setBusy(false);
    }
  };

  return (
    <View style={styles.root} testID="run-record">
      <Txt role="label" heading={2}>
        {t("runs.record_title")}
      </Txt>
      {steps.map((step) => {
        const said = detail(step, t);
        return (
          <View key={step.seq} style={[styles.row, { borderColor: colors.border.subtle }]} testID="record-step" {...webProps({ dataSet: { kind: step.kind } })}>
            <Txt role="body">{t(`runs.record.${step.kind}`)}</Txt>
            <Txt role="meta" tone="secondary" numeric>
              {[step.actor_id ? name(step.actor_id) : t("runs.record_system"), `${formatNumericDate(dateOf(step.at, DEFAULT_TIME_ZONE))} ${formatTime(step.at, DEFAULT_TIME_ZONE)}`].join(" · ")}
            </Txt>
            {said ? (
              <Txt role="meta" tone="secondary">
                {said}
              </Txt>
            ) : null}
          </View>
        );
      })}
      {problem ? <InlineBanner tone="warning" title={errorText(problem)} /> : null}
      {next != null ? <Button label={t("runs.record_more")} busy={busy} onPress={() => void loadMore()} testID="action-record-more" /> : null}
    </View>
  );
}

const styles = StyleSheet.create({
  root: { gap: 6 },
  row: { gap: 2, paddingVertical: 6, borderBottomWidth: 1 },
});
