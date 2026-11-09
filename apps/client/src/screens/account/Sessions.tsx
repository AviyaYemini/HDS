/**
 * The person's active sessions, with revoke and "sign out of other devices" (specs/007-ux-redesign FR-012).
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { Button, InlineBanner, SkeletonRows, StatusChip, Txt, showToast } from "../../design/components";
import { Icon } from "../../design/icons/Icon";
import { useTheme } from "../../design/theme/ThemeProvider";
import { DEFAULT_TIME_ZONE, formatHeaderDate, dateOf, formatTime } from "../../locale";
import { useLoad } from "../useLoad";
import { styles } from "./accountStyles";

export function Sessions() {
  const { t, i18n } = useTranslation();
  const session = useSession();
  const errorText = useErrorText();
  const { colors } = useTheme();
  const sessions = useLoad(() => call(session.api.GET("/me/sessions")));
  const [busy, setBusy] = useState<string | null>(null);
  const [bulkError, setBulkError] = useState<string | null>(null);
  const [renewed, setRenewed] = useState(false);
  const items = [...(sessions.data ?? [])].sort((a, b) => b.last_refreshed_at.localeCompare(a.last_refreshed_at));
  const revoke = async (id: string): Promise<void> => {
    setBusy(id);
    setRenewed(false);
    try {
      await call(session.api.DELETE("/me/sessions/{id}", { params: { path: { id } } }));
      showToast(t("account.sessions_ended", { count: 1 }));
      await sessions.reload();
    } catch (failure) {
      if (codeOf(failure) === "resource.not_found") {
        // A session's id is its refresh token, which rotates: the row went stale, not away. Reload
        // the list so the next press hits the live id (PR 2 review; a stable id is a follow-up).
        setRenewed(true);
        await sessions.reload();
      } else {
        showToast(errorText(codeOf(failure)), { tone: "warning" });
      }
    } finally {
      setBusy(null);
    }
  };
  // The server keeps the caller's session and ends the rest; when it cannot tell which session is
  // the caller's (409 session.current_unknown) it ends none and says so here.
  const signOutOthers = async (): Promise<void> => {
    setBusy("others");
    setBulkError(null);
    try {
      await call(session.api.POST("/auth/logout-others"));
      showToast(t("account.others_ended"));
      await sessions.reload();
    } catch (failure) {
      setBulkError(errorText(codeOf(failure)));
    } finally {
      setBusy(null);
    }
  };
  if (sessions.error) {
    return <InlineBanner tone="warning" title={errorText(sessions.error)} action={{ label: t("common.retry"), onPress: () => void sessions.reload() }} />;
  }
  if (sessions.data === null) return <SkeletonRows rows={2} testID="sessions-loading" />;
  const others = items.filter((item) => !item.current);
  return (
    <View style={styles.form}>
      {items.map((item) => (
        <View key={item.id} testID={`session-${item.id}`} style={[styles.sessionRow, { borderBottomColor: colors.border.subtle }]}>
          <Icon name={item.client_kind === "native" ? "phone" : "globe"} size={18} color={colors.text.secondary} />
          <View style={styles.sessionText}>
            <View style={styles.row}>
              <Txt role="label">{t(item.client_kind === "native" ? "account.device_phone" : "account.device_web")}</Txt>
              {item.current ? <StatusChip status="ready" label={t("account.this_device")} /> : null}
            </View>
            <Txt role="meta" tone="secondary" numeric>
              {t("account.last_used", {
                date: formatHeaderDate(dateOf(item.last_refreshed_at, DEFAULT_TIME_ZONE), i18n.language),
                time: formatTime(item.last_refreshed_at, DEFAULT_TIME_ZONE),
              })}
            </Txt>
          </View>
          {!item.current ? (
            <Button variant="quiet" label={t("account.end_session")} onPress={() => void revoke(item.id)} busy={busy === item.id} testID={`action-end-session-${item.id}`} />
          ) : null}
        </View>
      ))}
      {renewed ? <InlineBanner tone="info" title={t("account.session_renewed")} testID="session-renewed" /> : null}
      {bulkError ? <InlineBanner tone="warning" title={bulkError} testID="sign-out-others-error" /> : null}
      <Button
        variant="secondary"
        label={t("account.sign_out_others")}
        icon="sign-out"
        onPress={() => void signOutOthers()}
        disabled={others.length === 0}
        disabledReason={others.length === 0 ? t("account.no_other_devices") : undefined}
        busy={busy === "others"}
        testID="action-sign-out-others"
      />
    </View>
  );
}
