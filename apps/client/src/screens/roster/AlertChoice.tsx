/**
 * Who hears of a manual roster edit (specs/008-auto-scheduler-review T042g): everyone on the roster,
 * the people it affects, or no one.
 */
import { useTranslation } from "react-i18next";

import { SegmentedControl } from "../../design/components";
import type { Alert } from "./model";

export function AlertChoice({ value, onChange }: { value: Alert; onChange: (value: Alert) => void }) {
  const { t } = useTranslation();
  return (
    <SegmentedControl
      label={t("roster.alert_label")}
      value={value}
      onChange={onChange}
      options={[
        { value: "none", label: t("roster.alert_none") },
        { value: "affected", label: t("roster.alert_affected") },
        { value: "everyone", label: t("roster.alert_everyone") },
      ]}
      testID="edit-alert"
    />
  );
}
