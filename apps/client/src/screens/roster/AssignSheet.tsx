/**
 * The roster's assign sheet (specs/007-ux-redesign FR-030; 008 T042c, T042f, T042g, FR-034): people
 * searched by any part of the name and ranked as "who else could cover", each with its reason; who is
 * busy then, with a move, a swap or keeping both; a note to the person placed and who hears of it.
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { previewsEnabled } from "../../config/previews";
import { Button, EmptyState, Sheet, TextField, Txt } from "../../design/components";
import { DEFAULT_TIME_ZONE, formatTimeRange } from "../../locale";
import { numbersOf as coverNumbers, reasonKey } from "../runs/review";
import { AlertChoice } from "./AlertChoice";
import type { Alert, BusyOn, Cover, Person, Shift } from "./model";

export function AssignSheet({
  shift,
  people,
  busy,
  ranked,
  projectName,
  replacing,
  note,
  alert,
  onNote,
  onAlert,
  onClose,
  onChoose,
}: {
  shift: Shift | null;
  people: Person[];
  busy: BusyOn[];
  /** The shift's "Find someone" ranking (FR-034), when read; until then, by name. */
  ranked: Cover | null;
  /** A project's name, when the reader may see it; null otherwise. */
  projectName: (id: string | null) => string | null;
  /** The name of the person being replaced, when the sheet replaces someone (T042f). */
  replacing: string | null;
  note: string;
  alert: Alert;
  onNote: (note: string) => void;
  onAlert: (alert: Alert) => void;
  onClose: () => void;
  onChoose: (userId: string, how: "assign" | "move", busyOn: BusyOn | null) => void;
}) {
  const { t, i18n } = useTranslation();
  const [query, setQuery] = useState("");
  // Someone already on another shift then, chosen: the sheet asks what to do (T042f).
  const [pick, setPick] = useState<{ person: Person; on: BusyOn } | null>(null);
  const taken = new Set(shift?.assignments.map((a) => a.user_id) ?? []);
  const busyBy = new Map(busy.map((b) => [b.user_id, b]));
  // Search by any part of the name, ignoring case and accents (T042c). Later the list shows only the
  // project's own or qualified people, and search still finds everyone.
  const wanted = fold(query, i18n.language);
  // Ranked as "who else could cover" when the ranking is in: free people first, best first; by
  // name until then, and for anyone the ranking leaves out.
  const rank = new Map((ranked?.options ?? []).map((o, i) => [o.user_id, i]));
  const why = new Map((ranked?.options ?? []).map((o) => [o.user_id, o]));
  const options = people
    .filter((p) => !taken.has(p.id) && (wanted === "" || fold(p.full_name, i18n.language).includes(wanted)))
    .sort((a, b) => (rank.get(a.id) ?? 1e9) - (rank.get(b.id) ?? 1e9) || a.full_name.localeCompare(b.full_name, i18n.language));
  /** Where someone is already placed then: the project and the time, as far as the reader may see. */
  const busyText = (on: BusyOn): string => {
    if (!on.starts_at || !on.ends_at) return t("roster.busy_mark");
    const time = formatTimeRange(on.starts_at, on.ends_at, DEFAULT_TIME_ZONE);
    const project = projectName(on.project_id);
    return project ? t("roster.busy_where", { project, time }) : t("roster.busy_mark_at", { time });
  };
  const reasonOf = (id: string): string | null => {
    const o = why.get(id);
    if (!o) return null;
    // Someone busy then is said once, with where (busyText), not again as the ranking's reason.
    if (busyBy.has(id) && reasonKey(o.code ?? "", (key) => i18n.exists(key)) === "runs.reasons.overlap") return null;
    if (o.free) return t(o.close ? "review.cover_would_rather_not" : "review.cover_free");
    const numbers = coverNumbers(o, i18n.language);
    return [t(reasonKey(o.code ?? "", (key) => i18n.exists(key))), numbers ? t(numbers.key, numbers.values) : null].filter(Boolean).join(" · ");
  };
  const close = () => {
    setQuery("");
    setPick(null);
    onClose();
  };
  const choose = (person: Person) => {
    const on = busyBy.get(person.id);
    if (on) {
      setPick({ person, on });
      return;
    }
    setQuery("");
    onChoose(person.id, "assign", null);
  };
  const done = (how: "assign" | "move") => {
    const chosen = pick!;
    setPick(null);
    setQuery("");
    onChoose(chosen.person.id, how, chosen.on);
  };
  const title = replacing ? t("roster.replace_title", { person: replacing }) : t("roster.assign_title");
  return (
    <Sheet open={shift !== null} onClose={close} title={title} testID="assign-sheet">
      {pick ? (
        <View style={styles.warning} testID="assign-busy-choice">
          <Txt role="body">
            {pick.on.starts_at && pick.on.ends_at
              ? projectName(pick.on.project_id)
                ? t("roster.busy_at_where", { person: pick.person.full_name, project: projectName(pick.on.project_id), time: formatTimeRange(pick.on.starts_at, pick.on.ends_at, DEFAULT_TIME_ZONE) })
                : t("roster.busy_at", { person: pick.person.full_name, time: formatTimeRange(pick.on.starts_at, pick.on.ends_at, DEFAULT_TIME_ZONE) })
              : t("roster.busy_then", { person: pick.person.full_name })}
          </Txt>
          {pick.on.movable ? (
            <Button
              variant="primary"
              label={replacing ? t("roster.swap_with", { person: pick.person.full_name, other: replacing }) : t("roster.move_here", { person: pick.person.full_name })}
              onPress={() => done("move")}
              testID="action-busy-move"
            />
          ) : null}
          <Button variant="secondary" label={t("roster.keep_both", { person: pick.person.full_name })} onPress={() => done("assign")} testID="action-busy-keep" />
          <Button variant="quiet" label={t("common.cancel")} onPress={() => setPick(null)} testID="action-busy-cancel" />
        </View>
      ) : (
        <>
          <TextField label={t("roster.assign_search")} value={query} onChangeText={setQuery} testID="assign-search" autoFocus />
          {options.length === 0 ? (
            <Txt role="body" tone="secondary" testID="assign-none">
              {t("roster.assign_none")}
            </Txt>
          ) : null}
          {options.map((person) => (
            <View key={person.id} style={styles.option}>
              <Button variant="quiet" icon="user" label={person.full_name} testID="assign-option" web={{ dataSet: { userId: person.id, busy: String(busyBy.has(person.id)) } }} onPress={() => choose(person)} />
              {busyBy.has(person.id) ? (
                <Txt role="meta" tone="warning" testID="assign-busy">
                  {busyText(busyBy.get(person.id)!)}
                </Txt>
              ) : null}
              {reasonOf(person.id) ? (
                <Txt role="meta" tone={why.get(person.id)?.free ? "secondary" : "warning"} testID="assign-reason">
                  {reasonOf(person.id)}
                </Txt>
              ) : null}
            </View>
          ))}
        </>
      )}
      {/* A word to the person placed, shown on their own shift page, and who hears of it (T042g). */}
      <TextField label={t("roster.note_label")} value={note} onChangeText={onNote} maxLength={500} testID="edit-note" />
      <AlertChoice value={alert} onChange={onAlert} />
      {previewsEnabled ? (
        <EmptyState testID="preview-qualification-expiry" icon="star" badge={t("preview.badge")} title={t("preview.qualification_expiry_title")} body={t("preview.qualification_expiry_body")} feature={t("preview.comes_with", { feature: "014" })} />
      ) : null}
    </Sheet>
  );
}

/** ``text`` for matching: lower case, accents and Hebrew points removed, spaces squeezed. */
function fold(text: string, language: string): string {
  return text.normalize("NFD").replace(/[\u0300-\u036f\u0591-\u05c7]/g, "").toLocaleLowerCase(language).replace(/\s+/g, " ").trim();
}

const styles = StyleSheet.create({
  option: { gap: 2 },
  warning: { gap: 4, paddingVertical: 6 },
});
