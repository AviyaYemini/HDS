/**
 * My availability (US4): the open submission windows and their deadlines, the employee's entries,
 * and adding, changing and removing them. The API decides; its refusals are shown translated.
 * Blocked, open and close entries are read-only once their window has closed; absences are not.
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { call, codeOf, useErrorText } from "../api/errors";
import { useSession } from "../api/SessionContext";
import { Body, Button, Card, Chip, ErrorText, Field, Heading, Row, Screen, Title } from "../components/ui";
import { formatDay, formatInstant, formatWeekday, today } from "../time";
import {
  ABSENCE_KINDS,
  KINDS,
  canChange,
  inOpenWindows,
  kindsOffered,
  type AbsenceKind,
  type Entry,
  type Kind,
} from "./availability";
import { useLoad } from "./useLoad";

interface Draft {
  id: string | null;
  kind: Kind;
  absence_kind: AbsenceKind | null;
  starts_on: string;
  ends_on: string;
  shift_type_id: string | null;
  /** The kinds this draft may become. */
  choices: Kind[];
}

export function AvailabilityScreen() {
  const { t, i18n } = useTranslation();
  const errorText = useErrorText();
  const session = useSession();
  const { api } = session;
  const userId = session.user?.id ?? "";
  const path = { params: { path: { id: userId } } };

  // Past windows and entries can no longer change, so only the current ones are read.
  const page = useLoad(async () => {
    const from = today();
    const [windows, entries, types] = await Promise.all([
      call(api.GET("/api/v1/availability-windows", { params: { query: { from } } })),
      call(api.GET("/api/v1/employees/{id}/constraints", { params: { ...path.params, query: { from } } })),
      call(api.GET("/api/v1/shift-types")),
    ]);
    return { windows: windows.items, entries: entries.items, types: types.items };
  });

  const [draft, setDraft] = useState<Draft | null>(null);
  const [refusal, setRefusal] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  if (page.error !== null && page.data === null) {
    return (
      <Screen>
        <Title>{t("availability.title")}</Title>
        <ErrorText lines={[errorText(page.error)]} />
        <Button testID="retry" label={t("common.retry")} onPress={() => void page.reload()} quiet />
      </Screen>
    );
  }
  if (page.data === null) {
    return (
      <Screen>
        <Title>{t("availability.title")}</Title>
        <Body muted>{t("common.loading")}</Body>
      </Screen>
    );
  }

  const { windows, entries, types } = page.data;
  const open = windows.filter((w) => w.is_open);
  const offered = kindsOffered(windows);
  const typeName = (id: string | null) =>
    id === null ? t("availability.every_shift") : (types.find((type) => type.id === id)?.name ?? "");
  const activeTypes = types.filter((type) => type.is_active);

  async function act(request: () => Promise<unknown>): Promise<void> {
    setBusy(true);
    setRefusal(null);
    try {
      await request();
      setDraft(null);
      await page.reload();
    } catch (error) {
      setRefusal(codeOf(error));
    } finally {
      setBusy(false);
    }
  }

  function startAdding(): void {
    const first = open[0]?.starts_on ?? today();
    setRefusal(null);
    setDraft({
      id: null,
      kind: offered[0] ?? "absence",
      absence_kind: null,
      starts_on: first,
      ends_on: first,
      shift_type_id: null,
      choices: offered,
    });
  }

  function startChanging(entry: Entry): void {
    setRefusal(null);
    setDraft({
      id: entry.id,
      kind: entry.kind,
      absence_kind: entry.absence_kind,
      starts_on: entry.starts_on,
      ends_on: entry.ends_on,
      shift_type_id: entry.shift_type_id,
      // An absence outside an open window may change, but only into another absence.
      choices: inOpenWindows(entry.starts_on, entry.ends_on, windows) ? KINDS : offered,
    });
  }

  function save(current: Draft): Promise<void> {
    const absence = current.kind === "absence";
    const body = {
      kind: current.kind,
      absence_kind: absence ? current.absence_kind : null,
      starts_on: current.starts_on.trim(),
      ends_on: current.ends_on.trim(),
      shift_type_id: absence ? null : current.shift_type_id,
    };
    return act(() =>
      current.id === null
        ? call(api.POST("/api/v1/employees/{id}/constraints", { ...path, body }))
        : call(api.PATCH("/api/v1/constraints/{id}", { params: { path: { id: current.id } }, body })),
    );
  }

  function remove(id: string): Promise<void> {
    return act(() => call(api.DELETE("/api/v1/constraints/{id}", { params: { path: { id } } })));
  }

  return (
    <Screen>
      <Title>{t("availability.title")}</Title>

      {page.error !== null ? (
        <>
          <ErrorText lines={[errorText(page.error)]} />
          <Button testID="retry" label={t("common.retry")} onPress={() => void page.reload()} quiet />
        </>
      ) : null}

      <Heading>{t("availability.windows")}</Heading>
      {open.length === 0 ? (
        <Body muted>{t("availability.no_open_window")}</Body>
      ) : (
        open.map((w) => (
          <Card key={w.id} testID={`window-${w.id}`}>
            <Body>
              {t("availability.window_dates", {
                first: formatDay(w.starts_on, i18n.language),
                last: formatDay(w.ends_on, i18n.language),
              })}
            </Body>
            <Body muted>
              {t("availability.window_deadline", { deadline: formatInstant(w.deadline, i18n.language) })}
            </Body>
          </Card>
        ))
      )}

      <Heading>{t("availability.entries")}</Heading>
      <Card testID="entries">
        {entries.length === 0 ? <Body muted>{t("availability.no_entries")}</Body> : null}
        {entries.map((entry) => (
          <Card key={entry.id} testID={`entry-${entry.id}`}>
            <Row>
              <Body>{t(`availability.kind.${entry.kind}`)}</Body>
              <Body muted>
                {entry.kind === "absence"
                  ? entry.absence_kind === null
                    ? ""
                    : t(`availability.absence.${entry.absence_kind}`)
                  : typeName(entry.shift_type_id)}
              </Body>
            </Row>
            <Body muted>
              {t("availability.entry_dates", {
                first: formatDay(entry.starts_on, i18n.language),
                last: formatDay(entry.ends_on, i18n.language),
              })}
            </Body>
            {entry.weekday === null ? null : (
              <Body muted>{t("availability.on_weekday", { day: formatWeekday(entry.weekday, i18n.language) })}</Body>
            )}
            {canChange(entry, windows) ? (
              <Row>
                <Button
                  testID={`edit-${entry.id}`}
                  label={t("common.edit")}
                  onPress={() => startChanging(entry)}
                  quiet
                />
                <Button
                  testID={`remove-${entry.id}`}
                  label={t("common.remove")}
                  onPress={() => void remove(entry.id)}
                  disabled={busy}
                  quiet
                />
              </Row>
            ) : (
              <Body muted>{t("availability.read_only")}</Body>
            )}
          </Card>
        ))}
      </Card>

      {draft === null ? (
        <>
          {refusal !== null ? <ErrorText lines={[errorText(refusal)]} /> : null}
          <Button testID="add-entry" label={t("availability.add")} onPress={startAdding} />
        </>
      ) : (
        <Card testID="entry-form">
          <Heading>{draft.id === null ? t("availability.add") : t("availability.edit_title")}</Heading>
          <Body muted>{t("availability.kind_label")}</Body>
          <Row>
            {draft.choices.map((kind) => (
              <Chip
                key={kind}
                testID={`kind-${kind}`}
                label={t(`availability.kind.${kind}`)}
                selected={draft.kind === kind}
                onPress={() => setDraft({ ...draft, kind })}
              />
            ))}
          </Row>
          {draft.kind === "absence" ? (
            <>
              <Body muted>{t("availability.absence_kind_label")}</Body>
              <Row>
                {ABSENCE_KINDS.map((reason) => (
                  <Chip
                    key={reason}
                    testID={`absence-${reason}`}
                    label={t(`availability.absence.${reason}`)}
                    selected={draft.absence_kind === reason}
                    onPress={() => setDraft({ ...draft, absence_kind: reason })}
                  />
                ))}
              </Row>
            </>
          ) : (
            <>
              <Body muted>{t("availability.shift_type_label")}</Body>
              <Row>
                <Chip
                  testID="type-all"
                  label={t("availability.every_shift")}
                  selected={draft.shift_type_id === null}
                  onPress={() => setDraft({ ...draft, shift_type_id: null })}
                />
                {activeTypes.map((type) => (
                  <Chip
                    key={type.id}
                    testID={`type-${type.id}`}
                    label={type.name}
                    selected={draft.shift_type_id === type.id}
                    onPress={() => setDraft({ ...draft, shift_type_id: type.id })}
                  />
                ))}
              </Row>
            </>
          )}
          <Field
            testID="first-day"
            label={t("availability.first_day")}
            value={draft.starts_on}
            onChange={(starts_on) => setDraft({ ...draft, starts_on })}
          />
          <Field
            testID="last-day"
            label={t("availability.last_day")}
            value={draft.ends_on}
            onChange={(ends_on) => setDraft({ ...draft, ends_on })}
          />
          {refusal !== null ? <ErrorText lines={[errorText(refusal)]} /> : null}
          <Row>
            <Button testID="save-entry" label={t("common.save")} onPress={() => void save(draft)} disabled={busy} />
            <Button testID="cancel-entry" label={t("common.cancel")} onPress={() => setDraft(null)} quiet />
          </Row>
        </Card>
      )}
    </Screen>
  );
}
