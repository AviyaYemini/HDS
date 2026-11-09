/**
 * The run's plain controls and the manager's chips (specs/008-auto-scheduler-review FR-017, FR-020,
 * FR-021a). Each chip reads in words and can be removed; a new one is built from a kind, a person
 * and what the kind needs, from a fixed set, with no language model. At most 20.
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { Button, SearchablePicker, SegmentedControl, TextField, Txt } from "../../design/components";
import { useLoad } from "../useLoad";
import { CHIP_KINDS, MAX_CHIPS, chipWords, complete, needs, weekdayName, type Chip, type ChipKind, type Controls } from "./steering";

interface Props {
  controls: Controls;
  onControls: (controls: Controls) => void;
  chips: Chip[];
  onChips: (chips: Chip[]) => void;
  /** The run's projects, to keep someone on one. */
  projects: { id: string; name: string }[];
}

export function ChipEditor({ controls, onControls, chips, onChips, projects }: Props) {
  const { t, i18n } = useTranslation();
  const session = useSession();
  const loaded = useLoad(async () => {
    const [people, types] = await Promise.all([
      call(session.api.GET("/api/v1/employees", { params: { query: { is_active: true } } })),
      call(session.api.GET("/api/v1/shift-types")),
    ]);
    return {
      people: people.items.filter((p) => p.role !== "client").map((p) => ({ id: p.id, label: p.full_name })),
      types: types.items.map((s) => ({ id: s.id, label: s.name })),
    };
  }, []);
  const [draft, setDraft] = useState<Partial<Chip> | null>(null);
  const people = loaded.data?.people ?? [];
  const types = loaded.data?.types ?? [];
  const label = (list: { id: string; label: string }[], id: string) => list.find((x) => x.id === id)?.label ?? t("runs.unknown_person");
  const words = (chip: Chip) => {
    const said = chipWords(chip, (id) => label(people, id), (id) => projects.find((p) => p.id === id)?.name ?? "", (id) => label(types, id), i18n.language);
    return t(said.key, said.values);
  };
  const add = () => {
    if (draft && complete(draft)) onChips([...chips, draft]);
    setDraft(null);
  };
  const what = draft?.kind ? needs(draft.kind) : null;

  return (
    <View style={styles.root} testID="run-steering">
      <SegmentedControl
        label={t("runs.control.changes")}
        value={controls.keep_changes_small ? "small" : "free"}
        onChange={(v) => onControls({ ...controls, keep_changes_small: v === "small" })}
        options={[
          { value: "free", label: t("runs.control.rebalance") },
          { value: "small", label: t("runs.control.keep_small") },
        ]}
        testID="control-changes"
      />
      <SegmentedControl
        label={t("runs.control.wishes")}
        value={controls.wishes_strictly ? "strict" : "gentle"}
        onChange={(v) => onControls({ ...controls, wishes_strictly: v === "strict" })}
        options={[
          { value: "strict", label: t("runs.control.strictly") },
          { value: "gentle", label: t("runs.control.gently") },
        ]}
        testID="control-wishes"
      />
      <Txt role="label" heading={3}>
        {t("runs.chips_title")}
      </Txt>
      {chips.length === 0 ? (
        <Txt role="meta" tone="secondary">
          {t("runs.chips_none")}
        </Txt>
      ) : null}
      {chips.map((chip, index) => (
        <View key={`${chip.kind}-${chip.user_id}-${index}`} style={styles.chip} testID="run-chip">
          <Txt role="body" style={styles.grow}>
            {words(chip)}
          </Txt>
          <Button variant="quiet" icon="close" label={t("common.remove")} accessibilityLabel={t("runs.chip_remove", { chip: words(chip) })} onPress={() => onChips(chips.filter((_, i) => i !== index))} testID="action-remove-chip" />
        </View>
      ))}
      {draft ? (
        <View style={styles.builder} testID="chip-builder">
          <SearchablePicker label={t("runs.chip_kind")} items={CHIP_KINDS.map((k) => ({ id: k, label: t(`runs.chip_kind_name.${k}`) }))} value={draft.kind ?? null} onChange={(k) => setDraft({ user_id: draft.user_id, kind: k as ChipKind })} testID="chip-kind" />
          <SearchablePicker label={t("runs.chip_person")} items={people} value={draft.user_id ?? null} onChange={(id) => setDraft({ ...draft, user_id: id })} testID="chip-person" />
          {what === "count" ? (
            <TextField
              label={t(draft.kind === "max_hours" ? "runs.chip_hours" : "runs.chip_count")}
              value={draft.count != null ? String(draft.count) : ""}
              onChangeText={(v) => setDraft({ ...draft, count: /^\d+$/.test(v) ? Number(v) : undefined })}
              keyboardType="number-pad"
              testID="chip-count"
            />
          ) : null}
          {what === "place" ? (
            <SearchablePicker label={t("runs.chip_project")} items={projects.map((p) => ({ id: p.id, label: p.name }))} value={draft.project_id ?? null} onChange={(id) => setDraft({ ...draft, project_id: id })} testID="chip-project" />
          ) : null}
          {what === "when" ? (
            <SearchablePicker
              label={t("runs.chip_when")}
              items={[
                ...[0, 1, 2, 3, 4, 5, 6].map((d) => ({ id: `day:${d}`, label: weekdayName(d, i18n.language) })),
                ...types.map((s) => ({ id: `type:${s.id}`, label: s.label })),
              ]}
              value={draft.weekday != null ? `day:${draft.weekday}` : draft.shift_type_id ? `type:${draft.shift_type_id}` : null}
              onChange={(id) =>
                setDraft(id.startsWith("day:") ? { ...draft, weekday: Number(id.slice(4)), shift_type_id: undefined } : { ...draft, shift_type_id: id.slice(5), weekday: undefined })
              }
              testID="chip-when"
            />
          ) : null}
          <View style={styles.actions}>
            <Button variant="primary" label={t("runs.chip_add")} disabled={!complete(draft)} onPress={add} testID="action-chip-save" />
            <Button label={t("common.cancel")} onPress={() => setDraft(null)} testID="action-chip-cancel" />
          </View>
        </View>
      ) : (
        <Button
          icon="plus"
          label={t("runs.chip_new")}
          disabled={chips.length >= MAX_CHIPS}
          disabledReason={chips.length >= MAX_CHIPS ? t("runs.chips_full", { count: MAX_CHIPS }) : undefined}
          onPress={() => setDraft({})}
          testID="action-chip-new"
        />
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  root: { gap: 10 },
  chip: { flexDirection: "row", alignItems: "center", gap: 8 },
  grow: { flex: 1 },
  builder: { gap: 8 },
  actions: { flexDirection: "row", flexWrap: "wrap", gap: 8 },
});
