/**
 * My pay (005 US1): the gross pay earned so far in a month, before tax, a line per shift worked,
 * each with its start and end at the site and the rate it was paid at (FR-018).
 * Figures come from the API as strings and are shown as they come; pay a line does not have is said,
 * never shown as zero.
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { call, useErrorText } from "../api/errors";
import { useSession } from "../api/SessionContext";
import { MonthSwitch } from "../components/MonthSwitch";
import { Body, Button, Card, ErrorText, Row, Screen, Title } from "../components/ui";
import { currentMonth, formatDay, shiftTimes } from "../time";
import { useLoad } from "./useLoad";

export function MyPayScreen() {
  const { t, i18n } = useTranslation();
  const errorText = useErrorText();
  const { api } = useSession();
  const [month, setMonth] = useState(currentMonth);
  const pay = useLoad(() => call(api.GET("/me/pay", { params: { query: { month } } })), [month]);

  return (
    <Screen>
      <Title>{t("pay.title")}</Title>
      <MonthSwitch testID="pay" month={month} onChange={setMonth} />
      <Body muted>{t("pay.gross_note")}</Body>
      {pay.error !== null ? (
        <>
          <ErrorText lines={[errorText(pay.error)]} />
          <Button testID="retry" label={t("common.retry")} onPress={() => void pay.reload()} quiet />
        </>
      ) : pay.data === null ? (
        <Body muted>{t("common.loading")}</Body>
      ) : (
        <>
          <Card testID="pay-total">
            <Row>
              <Body>{t("pay.total_hours", { hours: pay.data.hours })}</Body>
              {pay.data.pay != null ? <Body>{t("pay.total_pay", { pay: pay.data.pay })}</Body> : null}
            </Row>
          </Card>
          {pay.data.shifts_missing_rate > 0 ? (
            <Card testID="pay-missing">
              <Body muted>{t("pay.missing_count", { count: pay.data.shifts_missing_rate })}</Body>
            </Card>
          ) : null}
          {pay.data.lines.length === 0 ? <Body>{t("pay.empty")}</Body> : null}
          {pay.data.lines.map((line) => {
            const times = shiftTimes(line.starts_at, line.ends_at, line.timezone, i18n.language);
            return (
            <Card key={line.shift_id} testID={`pay-line-${line.shift_id}`}>
              <Row>
                <Body>{formatDay(line.date, i18n.language)}</Body>
                <Body>{line.project_name}</Body>
                <Body>{t("pay.hours", { hours: line.hours })}</Body>
              </Row>
              <Row>
                <Body muted>
                  {times.endDay === null
                    ? t("shifts.time_range", { start: times.start, end: times.end })
                    : t("shifts.time_range_next_day", { start: times.start, end: times.end, endDay: times.endDay })}
                </Body>
                {line.base_rate !== null ? (
                  <Body muted>{t("pay.base_rate", { rate: line.base_rate })}</Body>
                ) : null}
              </Row>
              {line.parts.map((part) => (
                <Body key={part.rate_pct} muted>
                  {t("pay.part", { hours: part.hours, rate: Number(part.rate_pct).toString() })}
                </Body>
              ))}
              <Body>
                {!line.pay_enabled
                  ? t("pay.no_pay_on_project")
                  : line.rate_missing
                    ? t("pay.rate_missing")
                    : t("pay.line_pay", { pay: line.pay ?? "" })}
              </Body>
            </Card>
            );
          })}
        </>
      )}
    </Screen>
  );
}
