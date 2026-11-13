/**
 * One form pattern for every admin record (specs/007-ux-redesign FR-045, FR-046, T084).
 *
 * Each field is checked when it is left, never while typing; Save is never disabled, and a save
 * with errors shows a summary that takes focus and names each one, beside the fields' own messages.
 * Money is never saved on its own: a change of a money field asks first, in words that name the old
 * and new value and the date it applies from. Leaving with unsaved changes asks first.
 */
import { useRef, useState, type ComponentRef, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { codeOf, useErrorText } from "../../api/errors";
import { Button, InlineBanner, SearchablePicker, SegmentedControl, Sheet, TextField, Txt } from "../../design/components";
import type { PickerItem } from "../../design/components/SearchablePicker";
import { DateField } from "../../design/components/date/DateField";
import { useTheme } from "../../design/theme/ThemeProvider";
import { useUnsavedChanges } from "../../design/unsaved";
import { webProps } from "../../design/web";
import { DEFAULT_TIME_ZONE, formatMoney, formatNumericDate, todayIn } from "../../locale";

export type Values = Record<string, string>;

export interface FieldSpec {
  name: string;
  label: string;
  kind?: "text" | "email" | "money" | "percent" | "minutes" | "date" | "time" | "choice" | "pick" | "picks";
  required?: boolean;
  /** For a `choice`: two to five options, as a segmented control. */
  options?: readonly { value: string; label: string }[];
  /** For a `pick`: a long list (clients, sites), as the searchable picker; for `picks`, several, as toggles. */
  items?: readonly PickerItem[];
  /** Shown, checked and saved only while this holds (a temporary worker's dates). */
  when?: (values: Values) => boolean;
  /** For a pay field: what its change reaches, named in the confirmation (FR-046). */
  scope?: string;
  /** A further check of the value, given every value; the error's text, or null. */
  validate?: (value: string, values: Values) => string | null;
  /** Shown but not changed once the record exists (an e-mail, a time zone). */
  fixedOnceSaved?: boolean;
  helper?: string;
}

const MONEY = /^\d{1,7}(\.\d{1,4})?$/;

/**
 * An amount as the API stores it (four places, `75.0000`) shown with two when the rest are zeros
 * (`75.00`), never rounded: a rate with a real third or fourth place keeps it.
 */
export function moneyText(amount: string): string {
  return amount.replace(/^(-?\d+\.\d\d)0+$/, "$1");
}
const PERCENT = /^\d{1,3}(\.\d{1,2})?$/;
const MINUTES = /^\d{1,4}$/;
/** The kinds that are pay: a change of one is confirmed before it is saved (FR-046). */
const PAY_KINDS = new Set(["money", "percent"]);
const TIME = /^([01]\d|2[0-3]):[0-5]\d$/;
const EMAIL = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

/** The error a field's value has, or null (required, the kind's format, then its own check). */
export function fieldError(field: FieldSpec, values: Values, t: (key: string) => string): string | null {
  if (field.when && !field.when(values)) return null;
  const value = (values[field.name] ?? "").trim();
  if (value === "") return field.required ? t("form.required") : null;
  if (field.kind === "money" && !MONEY.test(value)) return t("form.money_invalid");
  if (field.kind === "percent" && !PERCENT.test(value)) return t("form.percent_invalid");
  if (field.kind === "minutes" && !MINUTES.test(value)) return t("form.minutes_invalid");
  if (field.kind === "time" && !TIME.test(value)) return t("form.time_invalid");
  if (field.kind === "email" && !EMAIL.test(value)) return t("form.email_invalid");
  return field.validate?.(value, values) ?? null;
}

/** The money fields whose value changed: what the confirmation names. */
export function moneyChanges(fields: readonly FieldSpec[], before: Values, after: Values): { field: FieldSpec; from: string; to: string }[] {
  return fields
    .filter((f) => PAY_KINDS.has(f.kind ?? "text") && (before[f.name] ?? "") !== (after[f.name] ?? ""))
    .map((field) => ({ field, from: before[field.name] ?? "", to: after[field.name] ?? "" }));
}

export interface RecordFormProps {
  fields: readonly FieldSpec[];
  initial: Values;
  /** Whether the record exists already (an edit), so `fixedOnceSaved` fields are read-only. */
  existing: boolean;
  onSave: (values: Values) => Promise<void>;
  /** Extra parts under the fields (a picker of people, a list). */
  children?: ReactNode;
  testID?: string;
}

export function RecordForm({ fields, initial, existing, onSave, children, testID = "record-form" }: RecordFormProps) {
  const { t, i18n } = useTranslation();
  const errorText = useErrorText();
  const { colors } = useTheme();
  const [values, setValues] = useState<Values>(initial);
  // What the form was opened or last saved with: unsaved means "differs from this" (group F review).
  const [baseline, setBaseline] = useState<Values>(initial);
  const [shownErrors, setShownErrors] = useState<Record<string, string>>({});
  const [summary, setSummary] = useState<string[] | null>(null);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [confirming, setConfirming] = useState<ReturnType<typeof moneyChanges> | null>(null);
  const summaryRef = useRef<ComponentRef<typeof View>>(null);

  const dirty = fields.some((f) => (values[f.name] ?? "") !== (baseline[f.name] ?? ""));
  useUnsavedChanges(dirty && !busy);

  // The latest values, for a field left right after a change, before the next render.
  const latest = useRef<Values>(initial);
  const set = (name: string, value: string) => {
    latest.current = { ...latest.current, [name]: value };
    setValues(latest.current);
  };
  const leave = (field: FieldSpec) => {
    const error = fieldError(field, latest.current, t);
    setShownErrors((shown) => {
      const next = { ...shown };
      if (error) next[field.name] = error;
      else delete next[field.name];
      return next;
    });
  };

  const save = async (): Promise<void> => {
    try {
      setBusy(true);
      setSaveError(null);
      const saved = latest.current;
      await onSave(saved);
      setBaseline(saved);
    } catch (failure) {
      setSaveError(codeOf(failure));
    } finally {
      setBusy(false);
    }
  };

  const trySave = () => {
    const errors: Record<string, string> = {};
    for (const field of fields) {
      const error = fieldError(field, latest.current, t);
      if (error) errors[field.name] = error;
    }
    setShownErrors(errors);
    if (Object.keys(errors).length > 0) {
      setSummary(fields.filter((f) => errors[f.name]).map((f) => `${f.label}: ${errors[f.name]}`));
      // After the summary draws, it takes focus, so it is the first thing read.
      setTimeout(() => (summaryRef.current as unknown as { focus?: () => void } | null)?.focus?.(), 0);
      return;
    }
    setSummary(null);
    const changes = existing ? moneyChanges(fields, baseline, latest.current) : [];
    if (changes.length > 0) {
      setConfirming(changes);
      return;
    }
    void save();
  };

  const today = formatNumericDate(todayIn(DEFAULT_TIME_ZONE));
  const payText = (field: FieldSpec, value: string): string =>
    !value ? t("form.none") : field.kind === "percent" ? `${value}%` : formatMoney(value, i18n.language);
  return (
    <View testID={testID} style={styles.form}>
      {summary ? (
        <View
          ref={summaryRef}
          testID="error-summary"
          role="alert"
          {...webProps({ tabIndex: -1 })}
          style={[styles.summary, { borderColor: colors.status.danger }]}
        >
          <Txt role="label">{t("form.summary_title", { count: summary.length })}</Txt>
          {summary.map((line) => (
            <Txt key={line} role="body">
              {line}
            </Txt>
          ))}
        </View>
      ) : null}
      {saveError ? <InlineBanner tone="warning" title={errorText(saveError)} /> : null}
      {fields.map((field) => {
        if (field.when && !field.when(values)) return null;
        const error = shownErrors[field.name] ?? null;
        const readOnly = existing && field.fixedOnceSaved === true;
        const label = field.required ? `${field.label} *` : field.label;
        let input: ReactNode;
        if (field.kind === "picks" && field.items) {
          // Several choices, a comma-separated list of ids, each a toggle (a temporary worker's projects).
          const chosen = new Set((values[field.name] ?? "").split(",").filter(Boolean));
          input = (
            <View testID={`field-${field.name}`} style={styles.field}>
              <Txt role="label">{label}</Txt>
              <View style={styles.picks}>
                {field.items.map((item) => {
                  const on = chosen.has(item.id);
                  return (
                    <Button
                      key={item.id}
                      variant={on ? "primary" : "secondary"}
                      icon={on ? "check" : undefined}
                      label={item.label}
                      disabled={readOnly}
                      web={{ "aria-pressed": String(on) }}
                      onPress={() => {
                        const next = new Set(chosen);
                        if (on) next.delete(item.id);
                        else next.add(item.id);
                        set(field.name, [...next].join(","));
                        leave(field);
                      }}
                      testID={`field-${field.name}-${item.id}`}
                    />
                  );
                })}
              </View>
              {error ? (
                <Txt role="meta" tone="danger" testID={`field-error-${field.name}`}>
                  {error}
                </Txt>
              ) : null}
            </View>
          );
        } else if (field.kind === "pick" && field.items) {
          input = (
            <SearchablePicker
              label={label}
              items={field.items}
              value={values[field.name] || null}
              onChange={(id) => {
                set(field.name, id);
                leave(field);
              }}
              disabled={readOnly}
              error={error}
              testID={`field-${field.name}`}
            />
          );
        } else if (field.kind === "choice" && field.options) {
          input = (
            <SegmentedControl
              label={field.label}
              options={field.options.map((o) => ({ value: o.value, label: o.label, testID: `field-${field.name}-${o.value}` }))}
              value={values[field.name] ?? field.options[0]!.value}
              onChange={(value) => set(field.name, value)}
              disabled={readOnly}
              testID={`field-${field.name}`}
            />
          );
        } else if (field.kind === "date") {
          input = (
            <DateField
              label={label}
              value={values[field.name] || null}
              onChange={(date) => {
                set(field.name, date ?? "");
                leave({ ...field });
              }}
              error={error}
              errorTestID={`field-error-${field.name}`}
              disabled={readOnly}
              testID={`field-${field.name}`}
            />
          );
        } else {
          input = (
            <TextField
              label={label}
              value={values[field.name] ?? ""}
              onChangeText={(text) => set(field.name, text)}
              onBlur={() => leave(field)}
              error={error}
              errorTestID={`field-error-${field.name}`}
              readOnly={readOnly}
              helper={field.helper}
              keyboardType={
                field.kind === "money" || field.kind === "percent" ? "decimal-pad" : field.kind === "minutes" ? "number-pad" : field.kind === "email" ? "email-address" : "default"
              }
              direction={field.kind && field.kind !== "text" ? "ltr" : undefined}
              autoCapitalize={field.kind === "email" ? "none" : undefined}
              testID={`field-${field.name}`}
            />
          );
        }
        return (
          <View key={field.name} style={styles.field}>
            {input}
          </View>
        );
      })}
      {children}
      <View style={styles.actions}>
        <Button variant="primary" icon="check" label={t("common.save")} onPress={trySave} busy={busy} testID="action-save" />
      </View>
      <Sheet open={confirming !== null} onClose={() => setConfirming(null)} title={t("form.confirm_money_title")} testID="confirm-dialog">
        <View style={styles.confirm}>
          {(confirming ?? []).map((change) => (
            <Txt key={change.field.name} role="body">
              {t("form.confirm_money", {
                field: change.field.label,
                from: payText(change.field, change.from),
                to: payText(change.field, change.to),
                date: today,
              })}
              {change.field.scope ? ` ${change.field.scope}` : ""}
            </Txt>
          ))}
          <View style={styles.actions}>
            <Button
              variant="primary"
              label={t("form.confirm_money_yes")}
              onPress={() => {
                setConfirming(null);
                void save();
              }}
              testID="action-confirm-save"
            />
            <Button label={t("common.cancel")} onPress={() => setConfirming(null)} testID="action-cancel-save" />
          </View>
        </View>
      </Sheet>
    </View>
  );
}

const styles = StyleSheet.create({
  form: { gap: 16 },
  field: { gap: 4 },
  picks: { flexDirection: "row", flexWrap: "wrap", gap: 8 },
  summary: { gap: 4, padding: 12, borderWidth: 2 },
  actions: { flexDirection: "row", flexWrap: "wrap", gap: 8 },
  confirm: { gap: 16 },
});
