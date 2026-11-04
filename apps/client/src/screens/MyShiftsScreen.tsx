/** My shifts (FR-123): the caller's published shifts, with times at each shift's site. */
import { useTranslation } from "react-i18next";

import { call, useErrorText } from "../api/errors";
import { useSession } from "../api/SessionContext";
import { Body, Button, Card, ErrorText, Row, Screen, Title } from "../components/ui";
import { shiftTimes, today } from "../time";
import { useLoad } from "./useLoad";

export function MyShiftsScreen() {
  const { t, i18n } = useTranslation();
  const errorText = useErrorText();
  const { api } = useSession();
  const shifts = useLoad(() => call(api.GET("/me/shifts", { params: { query: { from: today() } } })));

  return (
    <Screen>
      <Title>{t("shifts.title")}</Title>
      {shifts.error !== null ? (
        <>
          <ErrorText lines={[errorText(shifts.error)]} />
          <Button testID="retry" label={t("common.retry")} onPress={() => void shifts.reload()} quiet />
        </>
      ) : shifts.data === null ? (
        <Body muted>{t("common.loading")}</Body>
      ) : shifts.data.items.length === 0 ? (
        <Body>{t("shifts.empty")}</Body>
      ) : (
        shifts.data.items.map((shift) => {
          const times = shiftTimes(shift.starts_at, shift.ends_at, shift.timezone, i18n.language);
          return (
            <Card key={shift.id} testID={`shift-${shift.id}`}>
              <Row>
                <Body>{times.day}</Body>
                <Body>
                  {times.endDay === null
                    ? t("shifts.time_range", { start: times.start, end: times.end })
                    : t("shifts.time_range_next_day", {
                        start: times.start,
                        end: times.end,
                        endDay: times.endDay,
                      })}
                </Body>
              </Row>
              <Row>
                <Body>{shift.project.name}</Body>
                <Body muted>{shift.site.name}</Body>
                <Body muted>{shift.shift_type}</Body>
              </Row>
              <Body muted>{t("shifts.zone", { zone: shift.timezone })}</Body>
            </Card>
          );
        })
      )}
    </Screen>
  );
}
