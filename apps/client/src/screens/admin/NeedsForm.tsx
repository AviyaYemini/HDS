/**
 * One project's staffing needs as a form: people per shift template and weekday (004 FR-001).
 */
import { useTranslation } from "react-i18next";

import { call, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { InlineBanner, SkeletonRows, Txt } from "../../design/components";
import { weekdayNames } from "../../locale";
import { useLoad } from "../useLoad";
import { RecordForm, type FieldSpec, type Values } from "./RecordForm";

const WEEKDAYS = [0, 1, 2, 3, 4, 5, 6] as const;

/** One project's needs: people per shift template and weekday, saved together (004 FR-001). */
export function NeedsForm({ projectId, saved }: { projectId: string; saved: (id: string) => Promise<void> }) {
  const { t, i18n } = useTranslation();
  const errorText = useErrorText();
  const days = weekdayNames(i18n.language, "long");
  const { api } = useSession();
  const loaded = useLoad(async () => {
    const [types, needs] = await Promise.all([
      call(api.GET("/api/v1/projects/{id}/shift-types", { params: { path: { id: projectId } } })),
      call(api.GET("/api/v1/projects/{id}/requirements", { params: { path: { id: projectId } } })),
    ]);
    return { types: types.items, needs: needs.items };
  }, [projectId]);
  // A failed read says so, with a way to try again, rather than loading for good (CodeRabbit, PR 20).
  if (loaded.error) return <InlineBanner tone="warning" title={errorText(loaded.error)} action={{ label: t("common.retry"), onPress: () => void loaded.reload() }} />;
  if (loaded.data === null) return <SkeletonRows rows={3} />;
  const { types, needs } = loaded.data;
  if (types.length === 0) return <Txt role="body" tone="secondary">{t("needs.no_types")}</Txt>;
  const fields: FieldSpec[] = types.flatMap((type) =>
    WEEKDAYS.map((day) => ({
      name: `${type.id}|${day}`,
      label: `${type.name} · ${days[day]}`,
      validate: (value: string) => (/^\d{1,3}$/.test(value) ? null : t("form.count_invalid")),
    })),
  );
  const initial: Values = Object.fromEntries(fields.map((f) => [f.name, "0"]));
  for (const need of needs) initial[`${need.shift_type_id}|${need.weekday}`] = String(need.headcount);
  return (
    <RecordForm
      existing
      fields={fields}
      initial={initial}
      onSave={async (v) => {
        const items = Object.entries(v)
          .map(([key, value]) => {
            const [shift_type_id, weekday] = key.split("|");
            return { shift_type_id: shift_type_id!, weekday: Number(weekday), headcount: Number(value) };
          })
          .filter((item) => item.headcount > 0);
        await call(api.PUT("/api/v1/projects/{id}/requirements", { params: { path: { id: projectId } }, body: { items } }));
        await saved(projectId);
      }}
    />
  );
}
