/**
 * A shift's warnings (specs/007-ux-redesign FR-039 to FR-041; 008 T042d, R55): one row per person and
 * rule with its numbers in the UI's language, and an override with a reason. It only informs.
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { Button, Sheet, TextField, Txt, showToast } from "../../design/components";
import { groupWarnings, type Shift, type Warning } from "./model";

/** The text that says a warning's numbers, when the rule sends them. */
export function numbersOf(w: Pick<Warning, "values">): string | null {
  const values = w.values ?? {};
  if ("rest_hours" in values) return "roster.warning.rest";
  if ("maximum_hours" in values) return "roster.warning.weekly";
  return null;
}

export function WarningSheet({ shift, warnings, nameOf, onClose, onOverridden }: { shift: Shift | null; warnings: Warning[]; nameOf: (id: string) => string; onClose: () => void; onOverridden: (code: string, employee: string | null) => void }) {
  const { t } = useTranslation();
  const session = useSession();
  const errorText = useErrorText();
  const [overriding, setOverriding] = useState<Warning | null>(null);
  const [reason, setReason] = useState("");
  const confirm = async () => {
    if (!shift || !overriding) return;
    const assignment = shift.assignments.find((a) => a.user_id === overriding.employee_id);
    if (!assignment) return;
    try {
      await call(session.api.POST("/api/v1/assignments/{id}/overrides", { params: { path: { id: assignment.id } }, body: { warning_code: overriding.code, reason } }));
      onOverridden(overriding.code, overriding.employee_id);
      setOverriding(null);
      setReason("");
    } catch (failure) {
      showToast(errorText(codeOf(failure)), { tone: "warning" });
    }
  };
  return (
    <Sheet
      open={shift !== null}
      onClose={onClose}
      title={t("roster.warnings_title")}
      testID="warning-detail"
      footer={<Button variant="primary" label={t("roster.warnings_ok")} onPress={onClose} testID="action-warnings-ok" />}
    >
      {/* One row per person and rule, however many shifts it clashes with; one override each. */}
      {groupWarnings(warnings, (w) => w.employee_id ?? null).map(({ first: w, values }) => (
        <View key={`${w.employee_id ?? "shift"}-${w.code}`} style={styles.warning} testID="warning-row">
          <Txt role="label">{w.employee_id ? nameOf(w.employee_id) : t("roster.the_shift")}</Txt>
          <Txt role="body">{t(w.code)}</Txt>
          {numbersOf(w)
            ? values.map((v) => (
                // The rule and the numbers, in the UI's language (FR-041).
                <Txt key={JSON.stringify(v)} role="meta" tone="secondary" numeric>
                  {t(numbersOf(w)!, v)}
                </Txt>
              ))
            : null}
          {w.employee_id ? <Button variant="secondary" label={t("roster.override")} testID="action-override" onPress={() => setOverriding(w)} /> : null}
        </View>
      ))}
      {overriding ? (
        <View style={styles.warning}>
          <TextField label={t("roster.override_reason")} value={reason} onChangeText={setReason} testID="override-reason" />
          <Button variant="primary" label={t("roster.confirm_override")} disabled={reason.trim() === ""} onPress={() => void confirm()} testID="action-confirm-override" />
        </View>
      ) : null}
    </Sheet>
  );
}

const styles = StyleSheet.create({
  warning: { gap: 4, paddingVertical: 6 },
});
