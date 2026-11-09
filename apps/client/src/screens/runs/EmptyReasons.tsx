/**
 * Why a proposal's slot stays empty, in words (specs/008-auto-scheduler-review FR-013 to FR-016).
 */
import { useTranslation } from "react-i18next";
import { View } from "react-native";

import { Txt } from "../../design/components";
import { numbersOf, reasonKey, type Review, type ReviewSlot } from "./review";
import { styles } from "./reviewStyles";

/** An empty place of a run, with why, as the run reports it. */
interface RunGap {
  key: string;
  places: number;
  reasons: { code: string; employees: number; people?: ({ user_id: string } & Parameters<typeof numbersOf>[0])[] }[];
}

export function EmptyReasons({ review, slot, name }: { review: Review; slot: ReviewSlot; name: (id: string) => string }) {
  const { t, i18n } = useTranslation();
  const gap = (review.unfilled as unknown as RunGap[]).find((u) => u.key === slot.key);
  if (!gap) return null;
  return (
    <View testID="review-why-empty" style={styles.panel}>
      <Txt role="label" heading={3}>
        {t("review.why_empty")}
      </Txt>
      {gap.reasons.map((reason) => {
        const words = t(reasonKey(reason.code, (key) => i18n.exists(key)));
        return (
          <View key={reason.code}>
            <Txt role="meta" tone="secondary">
              {reason.employees > 0 ? t("runs.reason", { reason: words, count: reason.employees }) : words}
            </Txt>
            {(reason.people ?? []).map((person) => {
              const numbers = numbersOf(person, i18n.language);
              return (
                <Txt key={person.user_id} role="meta" tone="secondary" numeric>
                  {numbers ? t("runs.person_numbers", { person: name(person.user_id), numbers: t(numbers.key, numbers.values) }) : name(person.user_id)}
                </Txt>
              );
            })}
          </View>
        );
      })}
    </View>
  );
}

/** One empty slot of the run as the job stores it (the review sends it as it is). */
