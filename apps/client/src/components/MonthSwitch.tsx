/** A month and buttons to the previous and next one, for screens that show one month at a time. */
import { useTranslation } from "react-i18next";

import { addMonths, formatMonth } from "../time";
import { Button, Heading, Row } from "./ui";

export function MonthSwitch(props: { month: string; onChange: (month: string) => void; testID: string }) {
  const { t, i18n } = useTranslation();
  return (
    <Row>
      <Button
        testID={`${props.testID}-prev`}
        label={t("month.previous")}
        onPress={() => props.onChange(addMonths(props.month, -1))}
        quiet
      />
      <Heading>{formatMonth(props.month, i18n.language)}</Heading>
      <Button
        testID={`${props.testID}-next`}
        label={t("month.next")}
        onPress={() => props.onChange(addMonths(props.month, 1))}
        quiet
      />
    </Row>
  );
}
