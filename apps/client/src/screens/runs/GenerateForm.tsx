/**
 * The generate form (specs/007-ux-redesign FR-042; 008 FR-017 to FR-021a, FR-032): a range that cannot
 * start in the past, from the planning presets, the calendar or the roster's week; the projects
 * offered, never mixing training and security by default; the controls and chips; or a rerun.
 */
import { useRouter } from "expo-router";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { Button, DateRangeField, InlineBanner, SearchablePicker, Txt } from "../../design/components";
import { useTheme } from "../../design/theme/ThemeProvider";
import { DEFAULT_TIME_ZONE, addDays, compareDates, presetRange, todayIn, type DateRange } from "../../locale";
import { ChipEditor } from "./ChipEditor";
import { digest, typesOf } from "./model";
import { DEFAULT_CONTROLS, type Chip, type Controls } from "./steering";

/** A run to run again (FR-019, FR-021a): its range, projects, controls and chips, and its id. */
export interface Again {
  id: string;
  from: string;
  to: string;
  project_ids: string[];
  controls: Controls;
  chips: Chip[];
}

/** A run covers at most this many days (the API's limit, 004 FR-005). */
const MAX_DAYS = 31;

/** A project the generate form may offer: its name and type (training and security don't mix). */
export interface Offered {
  id: string;
  name: string;
  type: string;
}

/**
 * The roster's week as a run's range, never starting before today, as the date field allows: this
 * week from today on, or next week when the whole week has passed (FR-032).
 */
export function fromPreset(week: string, today: string): DateRange {
  const end = addDays(week, 6);
  if (compareDates(end, today) < 0) return presetRange("next-week", today);
  return { start: compareDates(week, today) < 0 ? today : week, end };
}

export function GenerateForm({ onCancel, offered, again, preset }: { onCancel: () => void; offered: Offered[]; again?: Again | null; preset?: { week: string; project: string | null } | null }) {
  const { t } = useTranslation();
  const session = useSession();
  const router = useRouter();
  const errorText = useErrorText();
  const { colors, isDesktop } = useTheme();
  const today = todayIn(DEFAULT_TIME_ZONE);
  const [range, setRange] = useState<DateRange>(() =>
    again ? { start: again.from, end: again.to } : preset ? fromPreset(preset.week, today) : presetRange("next-week", today),
  );
  // The two plain controls and the chips (FR-017, FR-020); a rerun starts with the earlier run's.
  const [controls, setControls] = useState<Controls>(() => again?.controls ?? DEFAULT_CONTROLS);
  const [chips, setChips] = useState<Chip[]>(() => again?.chips ?? []);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Every offered project at first when they are all of one type (every Manager today); with
  // both types offered, none, and a quick choice of each type: the default never mixes training
  // and security, though choosing both on purpose stays allowed (reviewer's ruling, 2026-11-08,
  // after the owner's note in spec.md's assumptions).
  const types = typesOf(offered);
  // From the roster's "Generate this week": its project, when the person may run it (FR-032).
  const [picked, setPicked] = useState<string[] | null>(() =>
    again ? again.project_ids : preset?.project && offered.some((p) => p.id === preset.project) ? [preset.project] : null,
  );
  const projects = picked ?? (types.length > 1 ? [] : offered.map((p) => p.id));
  // The key follows the request itself: a repeat of the same one (a double press, a retry after a
  // lost answer) answers the same run, and any change of range or projects (the offered list
  // arriving late included) is a new request, never a reused key (group O review).
  const [form] = useState(() => `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`);
  const key = `run-${form}-${digest(`${range.start}|${range.end}|${projects.join(",")}|${JSON.stringify(controls)}|${JSON.stringify(chips)}|${again?.id ?? ""}`)}`;
  const summary =
    projects.length === offered.length
      ? t("runs.projects_all", { count: offered.length })
      : projects.length === 0
        ? t("runs.projects_none")
        : offered.filter((p) => projects.includes(p.id)).map((p) => p.name).join(", ");
  const noProjects = projects.length === 0;

  const tooLong = compareDates(range.end, addDays(range.start, MAX_DAYS - 1)) > 0;

  const generate = async () => {
    setBusy(true);
    setError(null);
    try {
      const job = await call(
        session.api.POST("/api/v1/schedule-jobs", {
          body: { from: range.start, to: range.end, project_ids: projects, solver: "cpsat", controls, chips, ...(again ? { based_on: again.id } : {}) },
          params: { header: { "idempotency-key": key } },
        }),
      );
      router.push(`/runs/${job.id}`);
    } catch (failure) {
      setError(codeOf(failure));
      setBusy(false);
    }
  };

  return (
    // Unboxed on a phone, so the 308-pixel calendar fits the column (FR-079).
    <View testID="run-form" style={[styles.form, isDesktop ? null : styles.formPhone, { borderColor: colors.border.subtle, backgroundColor: colors.bg.surface }]}>
      <SearchablePicker
        label={t("runs.projects")}
        items={offered.map((p) => ({ id: p.id, label: p.name, secondary: t(`project.type.${p.type}`) }))}
        value={null}
        onChange={() => undefined}
        multiple={{ values: projects, onChange: setPicked, summary }}
        testID="run-projects"
      />
      {types.length > 1 ? (
        <View style={styles.formActions}>
          {types.map((type) => (
            <Button
              key={type}
              label={t(`runs.projects_of_type.${type}`)}
              onPress={() => setPicked(offered.filter((p) => p.type === type).map((p) => p.id))}
              testID={`action-projects-${type}`}
            />
          ))}
        </View>
      ) : null}
      <DateRangeField label={t("runs.range")} value={range} onChange={setRange} forward minDate={today} />
      <Txt role="meta" tone="secondary">
        {t("runs.range_hint")}
      </Txt>
      {again ? (
        <Txt role="meta" tone="secondary" testID="run-again-note">
          {t("runs.again_note")}
        </Txt>
      ) : null}
      <ChipEditor controls={controls} onControls={setControls} chips={chips} onChips={setChips} projects={offered.filter((p) => projects.includes(p.id))} />
      {error ? <InlineBanner tone="warning" title={errorText(error)} /> : null}
      <View style={styles.formActions}>
        <Button variant="primary" icon="play" label={t("runs.generate")} onPress={() => void generate()} busy={busy} disabled={tooLong || noProjects} disabledReason={tooLong ? t("schedule.range_invalid") : noProjects ? t("runs.projects_none") : undefined} testID="action-generate" />
        <Button label={t("common.cancel")} onPress={onCancel} testID="action-cancel-run" />
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  form: { gap: 12, padding: 16, borderWidth: 1 },
  formPhone: { padding: 0, borderWidth: 0 },
  formActions: { flexDirection: "row", flexWrap: "wrap", gap: 8 },
});
