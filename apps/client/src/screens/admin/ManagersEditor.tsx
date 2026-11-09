/**
 * Who manages a project, chosen by name: a search box (`searchable-picker`) whose matches show
 * each person's role and e-mail, in Hebrew or English. Only a Manager or an Admin may manage one, so
 * anyone else is listed with the reason and cannot be chosen.
 */
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { Button, TextField, Txt, matches as matchesItem, showToast } from "../../design/components";
import { useTheme } from "../../design/theme/ThemeProvider";
import { useLoad } from "../useLoad";

/** How many people the managers search lists before asking for more letters. */
const MATCHES_SHOWN = 8;

export function ManagersEditor({ projectId }: { projectId: string }) {
  const { t } = useTranslation();
  const { api } = useSession();
  const errorText = useErrorText();
  const { colors } = useTheme();
  const [query, setQuery] = useState("");
  const people = useLoad(async () => (await call(api.GET("/api/v1/employees", { params: { query: { is_active: true } } }))).items, []);
  const managers = useLoad(async () => (await call(api.GET("/api/v1/projects/{id}/managers", { params: { path: { id: projectId } } }))).items, [projectId]);
  const chosen = new Set((managers.data ?? []).map((m) => m.id));
  // The shared picker's matching (Hebrew and English, vowel points ignored), shown inline as the
  // routes contract has it (`searchable-picker`, a text input).
  const found = useMemo(() => {
    if (!query.trim()) return [];
    return (people.data ?? []).filter((p) => p.role !== "client" && matchesItem({ id: p.id, label: p.full_name, keywords: [p.email] }, query));
  }, [people.data, query]);
  const matches = found.slice(0, MATCHES_SHOWN);
  const replace = async (ids: string[]) => {
    try {
      await call(api.PUT("/api/v1/projects/{id}/managers", { params: { path: { id: projectId } }, body: { user_ids: ids } }));
      await managers.reload();
      setQuery("");
    } catch (failure) {
      showToast(errorText(codeOf(failure)), { tone: "warning" });
    }
  };
  return (
    <View style={styles.block}>
      <Txt role="label" heading={3}>
        {t("projects.managers")}
      </Txt>
      {(managers.data ?? []).map((m) => (
        <View key={m.id} style={styles.inline}>
          <Txt role="body" style={styles.grow}>
            {m.full_name}
          </Txt>
          <Button variant="quiet" label={t("common.remove")} onPress={() => void replace([...chosen].filter((id) => id !== m.id))} testID="action-remove-manager" />
        </View>
      ))}
      <TextField label={t("projects.add_manager")} value={query} onChangeText={setQuery} testID="searchable-picker" />
      {found.length > MATCHES_SHOWN ? (
        <Txt role="meta" tone="secondary">
          {t("projects.more_matches", { count: found.length - MATCHES_SHOWN })}
        </Txt>
      ) : null}
      {matches.map((person) => {
        const can = person.role === "manager" || person.role === "admin";
        return (
          <View key={person.id} testID="picker-option" style={[styles.option, { borderColor: colors.border.subtle }]}>
            <View style={styles.grow}>
              <Txt role="label">{person.full_name}</Txt>
              <Txt role="meta" tone="secondary" testID="picker-option-detail">
                {`${t(`role.${person.role}`)} · ${person.email}`}
              </Txt>
            </View>
            {chosen.has(person.id) ? (
              <Txt role="meta" tone="secondary">
                {t("projects.is_manager")}
              </Txt>
            ) : (
              <Button
                label={t("projects.make_manager")}
                disabled={!can}
                disabledReason={can ? undefined : t("project_managers.not_staff")}
                onPress={() => void replace([...chosen, person.id])}
                testID="action-add-manager"
              />
            )}
          </View>
        );
      })}
    </View>
  );
}

const styles = StyleSheet.create({
  block: { gap: 8 },
  inline: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 8 },
  grow: { flexGrow: 1, flexShrink: 1, flexBasis: 160 },
  option: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 8, padding: 8, borderWidth: 1 },
});
