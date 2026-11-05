/**
 * My pay (005 US1): the gross pay earned so far in a month, before tax, a line per shift worked.
 * Figures come from the API as strings and are shown as they come; pay a line does not have is said,
 * never shown as zero.
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { call, useErrorText } from "../api/errors";
import { useSession } from "../api/SessionContext";
import { MonthSwitch } from "../components/MonthSwitch";
import { Body, Button, Card, ErrorText, Row, Screen, Title } from "../components/ui";
import { currentMonth, formatDay } from "../time";
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
              <Body>{t("pay.total_pay", { pay: pay.data.pay })}</Body>
            </Row>
          </Card>
          {pay.data.shifts_missing_rate > 0 ? (
            <Card testID="pay-missing">
              <Body muted>{t("pay.missing_count", { count: pay.data.shifts_missing_rate })}</Body>
            </Card>
          ) : null}
          {pay.data.lines.length === 0 ? <Body>{t("pay.empty")}</Body> : null}
          {pay.data.lines.map((line) => (
            <Card key={line.shift_id} testID={`pay-line-${line.shift_id}`}>
              <Row>
                <Body>{formatDay(line.date, i18n.language)}</Body>
                <Body>{line.project_name}</Body>
                <Body>{t("pay.hours", { hours: line.hours })}</Body>
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
          ))}
        </>
      )}
    </Screen>
  );
}
