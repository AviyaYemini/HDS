/**
 * Someone on a shift, tapped on the roster (specs/007-ux-redesign FR-030; 008 FR-018, T042f, T042g):
 * take them off, lock or unlock the assignment for later runs, or replace them; and who hears of it.
 */
import { useTranslation } from "react-i18next";

import { Button, Sheet } from "../../design/components";
import { AlertChoice } from "./AlertChoice";
import type { Alert } from "./model";

interface Props {
  open: boolean;
  name: string;
  locked: boolean;
  alert: Alert;
  onAlert: (alert: Alert) => void;
  onClose: () => void;
  onUnassign: () => void;
  onLock: () => void;
  onReplace: () => void;
}

export function PersonSheet({ open, name, locked, alert, onAlert, onClose, onUnassign, onLock, onReplace }: Props) {
  const { t } = useTranslation();
  return (
    <Sheet open={open} onClose={onClose} title={name} testID="person-sheet">
      <AlertChoice value={alert} onChange={onAlert} />
      <Button variant="secondary" icon="close" label={t("roster.unassign")} testID="action-unassign" onPress={onUnassign} />
      {open ? <Button variant="secondary" icon="lock" label={locked ? t("roster.unlock") : t("roster.lock")} testID="action-lock" onPress={onLock} /> : null}
      <Button variant="secondary" icon="people" label={t("roster.replace")} testID="action-replace" onPress={onReplace} />
    </Sheet>
  );
}
