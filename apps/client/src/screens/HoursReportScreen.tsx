/**
 * Hours report (005 US5): hours and shifts by employee or by project for a month, with pay only
 * where the API sends it (an Admin, or a Manager on a project opened to them).
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { call, useErrorText } from "../api/errors";
import { useSession } from "../api/SessionContext";
import { MonthSwitch } from "../components/MonthSwitch";
import { Body, Button, Card, Chip, ErrorText, Heading, Row, Screen, Title } from "../components/ui";
import { currentMonth, monthBounds } from "../time";
import { useLoad } from "./useLoad";

type GroupBy = "employee" | "project";

export function HoursReportScreen() {
  const { t } = useTranslation();
  const errorText = useErrorText();
  const { api } = useSession();
  const [month, setMonth] = useState(currentMonth);
  const [groupBy, setGroupBy] = useState<GroupBy>("employee");
  const report = useLoad(() => {
    const { first, last } = monthBounds(month);
    return call(
      api.GET("/api/v1/reports/hours", { params: { query: { from: first, to: last, group_by: groupBy } } }),
    );
  }, [month, groupBy]);

  return (
    <Screen>
      <Title>{t("reports.hours")}</Title>
      <MonthSwitch testID="hours" month={month} onChange={setMonth} />
      <Row>
        <Chip
          testID="group-by-employee"
          label={t("reports.by_employee")}
          selected={groupBy === "employee"}
          onPress={() => setGroupBy("employee")}
        />
        <Chip
          testID="group-by-project"
          label={t("reports.by_project")}
          selected={groupBy === "project"}
          onPress={() => setGroupBy("project")}
        />
      </Row>
      {report.error !== null ? (
        <>
          <ErrorText lines={[errorText(report.error)]} />
          <Button testID="retry" label={t("common.retry")} onPress={() => void report.reload()} quiet />
        </>
      ) : report.data === null ? (
        <Body muted>{t("common.loading")}</Body>
      ) : (
        <>
          <Card testID="hours-total">
            <Row>
              <Body>{t("pay.total_hours", { hours: report.data.hours })}</Body>
              <Body>{t("reports.shifts", { count: report.data.shifts })}</Body>
              {report.data.pay !== undefined ? <Body>{t("pay.total_pay", { pay: report.data.pay })}</Body> : null}
            </Row>
            {report.data.shifts_missing_rate > 0 ? (
              <Body testID="hours-missing" muted>
                {t("pay.missing_count", { count: report.data.shifts_missing_rate })}
              </Body>
            ) : null}
          </Card>
          {report.data.groups.length === 0 ? <Body>{t("reports.nothing")}</Body> : null}
          {report.data.groups.map((group) => (
            <Card key={group.id} testID={`hours-group-${group.id}`}>
              <Row>
                <Heading>{group.name}</Heading>
                <Body>{t("reports.shifts", { count: group.shifts })}</Body>
                <Body>{t("pay.hours", { hours: group.hours })}</Body>
                {group.pay !== undefined ? <Body>{t("reports.pay", { pay: group.pay })}</Body> : null}
              </Row>
              {group.rows.map((row) => (
                <Row key={row.id}>
                  <Body muted>{row.name}</Body>
                  <Body muted>{t("reports.shifts", { count: row.shifts })}</Body>
                  <Body muted>{t("pay.hours", { hours: row.hours })}</Body>
                  {row.pay !== undefined ? <Body muted>{t("reports.pay", { pay: row.pay })}</Body> : null}
                </Row>
              ))}
            </Card>
          ))}
        </>
      )}
    </Screen>
  );
}
