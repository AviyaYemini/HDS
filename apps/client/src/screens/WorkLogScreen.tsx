/**
 * Work-log (005 US4): who worked at a project, a site or a group. For a project the range starts at
 * its start (at most a year back) and ends today unless dates are given; a site or a group needs a
 * start date. Managers and Admins only; it never shows pay.
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { call, useErrorText } from "../api/errors";
import { useSession } from "../api/SessionContext";
import { Body, Button, Card, Chip, ErrorText, Field, Heading, Row, Screen, Title } from "../components/ui";
import { formatDay } from "../time";
import { useLoad } from "./useLoad";

type Kind = "project" | "site" | "group";
const KINDS: readonly Kind[] = ["project", "site", "group"];

interface Choice {
  id: string;
  name: string;
}

export function WorkLogScreen() {
  const { t, i18n } = useTranslation();
  const errorText = useErrorText();
  const { api } = useSession();
  const [kind, setKind] = useState<Kind>("project");
  const [chosen, setChosen] = useState<string | null>(null);
  const [first, setFirst] = useState("");
  const [last, setLast] = useState("");
  const [asked, setAsked] = useState<{ first: string; last: string }>({ first: "", last: "" });

  const choices = useLoad<Choice[]>(async () => {
    if (kind === "project") return (await call(api.GET("/api/v1/projects"))).items;
    if (kind === "site") return (await call(api.GET("/api/v1/sites"))).items;
    return (await call(api.GET("/api/v1/groups"))).items;
  }, [kind]);
  const needsStart = kind !== "project" && !asked.first;
  const log = useLoad(
    async () => {
      if (chosen === null || needsStart) return null;
      const query: { project_id?: string; site_id?: string; group_id?: string; from?: string; to?: string } = {
        [`${kind}_id`]: chosen,
      };
      if (asked.first) query.from = asked.first;
      if (asked.last) query.to = asked.last;
      return call(api.GET("/api/v1/reports/work-log", { params: { query } }));
    },
    [kind, chosen, asked.first, asked.last],
  );

  function pickKind(next: Kind) {
    setKind(next);
    setChosen(null);
  }

  return (
    <Screen>
      <Title>{t("reports.work_log")}</Title>
      <Row>
        {KINDS.map((k) => (
          <Chip key={k} testID={`kind-${k}`} label={t(`reports.kind.${k}`)} selected={k === kind} onPress={() => pickKind(k)} />
        ))}
      </Row>
      {choices.error !== null ? (
        <>
          <ErrorText lines={[errorText(choices.error)]} />
          <Button testID="retry-choices" label={t("common.retry")} onPress={() => void choices.reload()} quiet />
        </>
      ) : (
        <>
          <Heading>{t(`reports.kind.${kind}`)}</Heading>
          <Row>
            {(choices.data ?? []).map((c) => (
              <Chip key={c.id} testID={`${kind}-${c.id}`} label={c.name} selected={c.id === chosen} onPress={() => setChosen(c.id)} />
            ))}
          </Row>
        </>
      )}
      <Row>
        <Field
          testID="log-from"
          label={kind === "project" ? t("reports.from_optional") : t("reports.from_required")}
          value={first}
          onChange={setFirst}
        />
        <Field testID="log-to" label={t("reports.to_optional")} value={last} onChange={setLast} />
        <Button testID="log-apply" label={t("reports.show")} onPress={() => setAsked({ first, last })} quiet />
      </Row>
      {chosen === null ? (
        <Body muted>{t(`reports.choose.${kind}`)}</Body>
      ) : needsStart ? (
        <Body muted>{t("reports.start_required")}</Body>
      ) : log.error !== null ? (
        <>
          <ErrorText lines={[errorText(log.error)]} />
          <Button testID="retry" label={t("common.retry")} onPress={() => void log.reload()} quiet />
        </>
      ) : log.data === null ? (
        <Body muted>{t("common.loading")}</Body>
      ) : (
        <>
          <Body testID="log-range" muted>
            {t("reports.range", {
              from: formatDay(log.data.from, i18n.language),
              to: formatDay(log.data.to, i18n.language),
            })}
          </Body>
          {log.data.rows.length === 0 ? <Body>{t("reports.nobody")}</Body> : null}
          {log.data.rows.map((row) => (
            <Card key={`${row.user_id}-${row.project_id}`} testID={`log-row-${row.user_id}-${row.project_id}`}>
              <Row>
                <Body>{row.full_name}</Body>
                <Body muted>{row.project_name}</Body>
              </Row>
              <Row>
                <Body>{t("reports.shifts", { count: row.shifts })}</Body>
                <Body>{t("pay.hours", { hours: row.hours })}</Body>
              </Row>
              <Body muted>
                {t("reports.range", {
                  from: formatDay(row.first_date, i18n.language),
                  to: formatDay(row.last_date, i18n.language),
                })}
              </Body>
            </Card>
          ))}
        </>
      )}
    </Screen>
  );
}
