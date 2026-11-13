/**
 * The publish review (specs/007-ux-redesign FR-034 to FR-036): the week's shifts, people, empty places
 * (no warnings: out of scope, the owner, 2026-11-13), and whether everyone is told; then publish.
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Pressable, StyleSheet, View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { Button, Sheet, SkeletonRows, Txt, showToast } from "../../design/components";
import { useTheme } from "../../design/theme/ThemeProvider";
import { useLoad } from "../useLoad";

export function PublishSheet({ open, project, first, last, onClose, onPublished }: { open: boolean; project: string; first: string; last: string; onClose: () => void; onPublished: () => Promise<void> }) {
  const { t } = useTranslation();
  const session = useSession();
  const errorText = useErrorText();
  const [notify, setNotify] = useState(true);
  const [busy, setBusy] = useState(false);
  const preview = useLoad(
    async () => (open ? call(session.api.GET("/api/v1/projects/{id}/publish-preview", { params: { path: { id: project }, query: { from: first, to: last } } })) : null),
    [open, project, first],
  );
  const publish = async () => {
    setBusy(true);
    try {
      await call(session.api.POST("/api/v1/projects/{id}/publish", { params: { path: { id: project } }, body: { from: first, to: last, notify } }));
      await onPublished();
    } catch (failure) {
      showToast(errorText(codeOf(failure)), { tone: "warning" });
    } finally {
      setBusy(false);
    }
  };
  const counts = preview.data;
  return (
    <Sheet open={open} onClose={onClose} title={t("roster.publish_title")} testID="publish-review">
      {counts === null ? (
        <SkeletonRows rows={3} />
      ) : (
        <View style={styles.counts}>
          <Count label={t("roster.review_shifts")} value={counts.shifts} testID="review-shifts" />
          <Count label={t("roster.review_people")} value={counts.people} testID="review-people" />
          <Count label={t("roster.review_unfilled")} value={counts.unfilled} testID="review-unfilled" />
        </View>
      )}
      <View role="radiogroup" accessibilityLabel={t("roster.notify_label")} style={styles.radios}>
        <Radio checked={notify} label={t("roster.notify_everyone")} testID="notify-everyone" onPress={() => setNotify(true)} />
        <Radio checked={!notify} label={t("roster.publish_silently")} testID="publish-silently" onPress={() => setNotify(false)} />
      </View>
      <Button variant="primary" icon="check" label={t("roster.confirm_publish")} busy={busy} disabled={counts === null} onPress={() => void publish()} testID="action-confirm-publish" />
    </Sheet>
  );
}

function Count({ label, value, testID }: { label: string; value: number; testID: string }) {
  return (
    <View style={styles.count}>
      <Txt role="meta" tone="secondary">
        {label}
      </Txt>
      <Txt role="section" numeric testID={testID}>
        {String(value)}
      </Txt>
    </View>
  );
}

function Radio({ checked, label, testID, onPress }: { checked: boolean; label: string; testID: string; onPress: () => void }) {
  const { colors } = useTheme();
  return (
    <Pressable role="radio" aria-checked={checked} accessibilityLabel={label} testID={testID} onPress={onPress} style={[styles.radio, { borderColor: checked ? colors.accent.default : colors.border.subtle }]}>
      <Txt role="label">{label}</Txt>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  counts: { flexDirection: "row", flexWrap: "wrap", gap: 16 },
  count: { gap: 2, minWidth: 96 },
  radios: { flexDirection: "row", flexWrap: "wrap", gap: 8 },
  radio: { minHeight: 44, borderWidth: 1, paddingHorizontal: 12, justifyContent: "center" },
});
