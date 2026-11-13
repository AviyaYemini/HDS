/**
 * Staff: the people of the organisation as a table with a record panel (specs/007-ux-redesign FR-043 to FR-049).
 * Staff control for the CEO demo (specs/demo-ceo/plan.md, screen 3; slide 8): this week's and this
 * month's hours from the hours report by employee (no weekly-limit flag: warnings are out of scope), a permanent or
 * temporary filter (`?type=`), and a "temporary · until 12.02" chip.
 */
import { router, useLocalSearchParams } from "expo-router";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { useCapabilities } from "../../capabilities";
import { Button, SegmentedControl, Sheet, StatusChip, Txt, showToast, type Column } from "../../design/components";
import { useTheme } from "../../design/theme/ThemeProvider";
import { DEFAULT_TIME_ZONE, addDays, formatMoney, formatNumericDate, startOfWeek, todayIn } from "../../locale";
import { periodRange } from "../overview/model";
import { allShifts } from "../roster/allShifts";
import { useLoad } from "../useLoad";
import { AdminTable } from "./AdminTable";
import { RecordForm, moneyText, type FieldSpec } from "./RecordForm";
import { dayMonth, employmentOf, endingSoon, initials, maxEngagedUntil, passesType, scheduledHours, typeFilterOf, type TypeFilter } from "./staffModel";
import { orNull } from "./text";

/** Hours by person over `from`–`to`, from the hours report (published shifts that are over). */
function useHoursByPerson(from: string, to: string): Map<string, number> | null {
  const { api } = useSession();
  const report = useLoad(async () => (await call(api.GET("/api/v1/reports/hours", { params: { query: { from, to, group_by: "employee" } } }))).groups, [from, to]);
  // A failed report reads as no hours ("—"), never as loading for ever; the table still works.
  if (report.error) return new Map();
  return report.data ? new Map(report.data.map((g) => [g.id, Number(g.hours)])) : null;
}

export function StaffScreen() {
  const { t, i18n } = useTranslation();
  const { colors } = useTheme();
  const session = useSession();
  const { api } = session;
  const capabilities = useCapabilities();
  type Row = Awaited<ReturnType<typeof loadStaff>>[number];
  async function loadStaff() {
    return (await call(api.GET("/api/v1/employees"))).items.filter((u) => u.role !== "client");
  }
  const params = useLocalSearchParams<{ type?: string }>();
  const typeFilter = typeFilterOf(params.type);
  const today = todayIn(DEFAULT_TIME_ZONE);
  const week = startOfWeek(today);
  const month = periodRange("month", today);
  // This week: rostered hours, drafts included.
  const weekShifts = useLoad(() => allShifts(api, week, addDays(week, 6)), [week]);
  const weekHours = weekShifts.error ? new Map<string, number>() : weekShifts.data ? scheduledHours(weekShifts.data) : null;
  // This month: hours worked, from the report (never past today; only shifts that are over).
  const monthHours = useHoursByPerson(month.from, today);
  const hoursText = (map: Map<string, number> | null, id: string) => (map === null ? "…" : map.has(id) ? t("pay.hours", { hours: map.get(id)!.toFixed(1).replace(/\.0$/, "") }) : "—");
  const roles = ["employee", "manager", "admin"] as const;
  // The projects a temporary worker can be on (CEO demo §1): the active ones.
  const projects = useLoad(async () => (await call(api.GET("/api/v1/projects"))).items.filter((p) => p.status === "active"), []);
  const temporary = (v: Record<string, string>) => v.employment_type === "temporary";
  const employmentFields: FieldSpec[] = [
    { name: "employment_type", label: t("staff.employment"), kind: "choice", options: (["permanent", "temporary"] as const).map((v) => ({ value: v, label: t(`staff.kind.${v}`) })) },
    { name: "engaged_from", label: t("staff.engaged_from"), kind: "date", required: true, when: temporary },
    {
      name: "engaged_until",
      label: t("staff.engaged_until"),
      kind: "date",
      required: true,
      when: temporary,
      helper: t("staff.engaged_until_help"),
      validate: (until, v) => {
        const from = v.engaged_from ?? "";
        if (!from) return null;
        if (until < from) return t("staff.until_before_from");
        const latest = maxEngagedUntil(from);
        return until > latest ? t("staff.until_too_far", { date: formatNumericDate(latest) }) : null;
      },
    },
    { name: "project_ids", label: t("staff.projects"), kind: "picks", required: true, when: temporary, items: (projects.data ?? []).map((p) => ({ id: p.id, label: p.name })) },
  ];
  const fields = (existing: boolean): FieldSpec[] => [
    { name: "full_name", label: t("entity.full_name"), required: true },
    { name: "email", label: t("entity.email"), kind: "email", required: true, fixedOnceSaved: true },
    { name: "role", label: t("entity.role"), kind: "choice", options: roles.map((r) => ({ value: r, label: t(`role.${r}`) })) },
    { name: "phone", label: t("entity.phone") },
    { name: "default_hourly_rate", label: t("entity.default_hourly_rate"), kind: "money", scope: t("scope.hourly_rate") },
    ...(existing ? [] : [{ name: "initial_password", label: t("entity.initial_password"), required: true, helper: t("entity.initial_password_help") }]),
    ...employmentFields,
  ];
  const columns: Column<Row>[] = [
    {
      key: "name",
      title: t("entity.full_name"),
      sortable: true,
      text: (u) => u.full_name,
      // An initials avatar beside the name (the owner, 2026-11-13).
      render: (u) => (
        <View style={styles.nameCell}>
          <View style={[styles.avatar, { backgroundColor: colors.bg.selected, borderColor: colors.border.subtle }]} accessibilityElementsHidden importantForAccessibility="no-hide-descendants">
            <Txt role="eyebrow" tone="secondary">
              {initials(u.full_name)}
            </Txt>
          </View>
          <View style={styles.name}>
            <Txt role="label" numberOfLines={1}>
              {u.full_name}
            </Txt>
            {/* The e-mail as a quiet second line, not a column of its own (the helper, 2026-11-13). */}
            <Txt role="meta" tone="secondary" numberOfLines={1} dir="ltr">
              {u.email}
            </Txt>
          </View>
        </View>
      ),
    },
    { key: "role", title: t("entity.role"), sortable: true, width: 110, render: (u) => t(`role.${u.role}`) },
    { key: "rate", title: t("entity.default_hourly_rate"), sortable: true, numeric: true, width: 96, render: (u) => (u.default_hourly_rate ? formatMoney(moneyText(u.default_hourly_rate), i18n.language) : "—") },
    {
      key: "week",
      title: t("staff.col.week"),
      sortable: true,
      numeric: true,
      width: 90,
      render: (u) => hoursText(weekHours, u.id),
    },
    { key: "month", title: t("staff.col.month"), sortable: true, numeric: true, width: 90, render: (u) => hoursText(monthHours, u.id) },
    {
      key: "active",
      title: t("entity.status"),
      sortable: true,
      width: 210,
      text: (u) => {
        const job = employmentOf(u);
        const temp = job.employment_type === "temporary" ? (job.engaged_until ? t("staff.temporary_until", { date: dayMonth(job.engaged_until) }) : t("staff.temporary")) : "";
        const soon = endingSoon(u, today);
        return [u.is_active ? t("entity.active") : t("entity.inactive"), temp, soon === null ? "" : t("staff.ending_soon", { count: soon })].filter(Boolean).join(" · ");
      },
      render: (u) => {
        const job = employmentOf(u);
        return (
          <View style={styles.chips}>
            <StatusChip status={u.is_active ? "success" : "neutral"} label={u.is_active ? t("entity.active") : t("entity.inactive")} />
            {job.employment_type === "temporary" ? (
              <StatusChip status="info" label={job.engaged_until ? t("staff.temporary_until", { date: dayMonth(job.engaged_until) }) : t("staff.temporary")} testID={`chip-temporary-${u.id}`} />
            ) : null}
            {endingSoon(u, today) !== null ? <StatusChip status="warning" label={t("staff.ending_soon", { count: endingSoon(u, today)! })} testID={`chip-ending-${u.id}`} /> : null}
          </View>
        );
      },
    },
  ];
  const [inviting, setInviting] = useState(false);
  const setType = (type: TypeFilter) => router.setParams({ type: type === "all" ? undefined : type } as never);
  const inviteFields: FieldSpec[] = [
    { name: "email", label: t("entity.email"), kind: "email", required: true },
    { name: "role", label: t("entity.role"), kind: "choice", options: roles.map((r) => ({ value: r, label: t(`role.${r}`) })) },
    ...employmentFields,
  ];
  return (
    <>
    <Sheet open={inviting} onClose={() => setInviting(false)} title={t("invite.title")} testID="invite-sheet">
      <RecordForm
        key={inviting ? "open" : "closed"}
        existing={false}
        fields={inviteFields}
        testID="invite-form"
        initial={{ email: "", role: "employee", employment_type: "permanent", engaged_from: "", engaged_until: "", project_ids: "" }}
        onSave={async (v) => {
          // Regular unless "temporary" is chosen; a temporary invitee needs the dates and projects.
          const job = v.employment_type === "temporary" ? { employment_type: "temporary" as const, engaged_from: v.engaged_from, engaged_until: v.engaged_until, project_ids: (v.project_ids ?? "").split(",").filter(Boolean) } : {};
          await call(api.POST("/auth/invite", { body: { email: v.email!.trim(), role: v.role as (typeof roles)[number], ...job } }));
          setInviting(false);
          showToast(t("invite.sent", { email: v.email!.trim() }), { tone: "success" });
        }}
      />
    </Sheet>
    <AdminTable<Row>
      screen="admin_staff"
      testID="screen-admin-staff"
      title={t("nav.staff")}
      noun={t("entity.noun.staff")}
      icon="people"
      columns={columns}
      load={loadStaff}
      keyOf={(u) => u.id}
      nameOf={(u) => u.full_name}
      searchText={(u) => `${u.full_name} ${u.email} ${u.phone ?? ""}`}
      sortValue={(u, key) => (key === "week" ? (weekHours?.get(u.id) ?? 0) : key === "month" ? (monthHours?.get(u.id) ?? 0) : key === "rate" ? (u.default_hourly_rate ? Number(u.default_hourly_rate) : null) : key === "email" ? u.email : key === "role" ? u.role : key === "active" ? (u.is_active ? 1 : 0) : u.full_name)}
      emptyTitle={t("admin.empty.staff_title")}
      emptyBody={t("admin.empty.staff_body")}
      // This month's hours first, the most first: who is working, not one-off names (the helper, 2026-11-13).
      defaultSort={{ key: "month", direction: "desc" }}
      filter={(u) => passesType(u, typeFilter)}
      // A phone's card: the role and e-mail, the hours this week and month and the rate, and the chips.
      phoneCard={(u) => {
        const job = employmentOf(u);
        return {
          subtitle: `${t(`role.${u.role}`)} · ${u.email}`,
          figures: [
            { label: t("staff.col.week"), value: hoursText(weekHours, u.id) },
            { label: t("staff.col.month"), value: hoursText(monthHours, u.id) },
            ...(u.default_hourly_rate ? [{ label: t("entity.default_hourly_rate"), value: formatMoney(moneyText(u.default_hourly_rate), i18n.language) }] : []),
          ],
          chips: (
            <>
              <StatusChip status={u.is_active ? "success" : "neutral"} label={u.is_active ? t("entity.active") : t("entity.inactive")} />
              {job.employment_type === "temporary" ? (
                <StatusChip status="info" label={job.engaged_until ? t("staff.temporary_until", { date: dayMonth(job.engaged_until) }) : t("staff.temporary")} testID={`card-temporary-${u.id}`} />
              ) : null}
              {endingSoon(u, today) !== null ? <StatusChip status="warning" label={t("staff.ending_soon", { count: endingSoon(u, today)! })} testID={`card-ending-${u.id}`} /> : null}
            </>
          ),
        };
      }}
      toolbar={
        <>
          <SegmentedControl<TypeFilter>
            label={t("staff.type_filter")}
            options={(["all", "permanent", "temporary"] as const).map((v) => ({ value: v, label: t(`staff.type.${v}`), testID: `type-${v}` }))}
            value={typeFilter}
            onChange={setType}
            testID="staff-type-filter"
          />
          {/* Invite by e-mail, regular unless an Admin marks the invitee temporary (the owner, 2026-11-13). */}
          <Button icon="mail" label={t("invite.open")} onPress={() => setInviting(true)} testID="action-invite" />
        </>
      }
      // No temporary worker yet: say how one is marked, never a bare "no match".
      filteredEmpty={typeFilter === "temporary" ? t("staff.no_temporary") : null}
      active={{
        of: (u) => u.is_active,
        // Never your own account: a bulk deactivation would sign you out partway (code review, PR 5).
        keep: (u) => (u.id === session.user?.id ? t("admin.not_yourself") : null),
        set: async (u, to) => void (await call(to ? api.POST("/api/v1/employees/{id}/reactivate", { params: { path: { id: u.id } } }) : api.POST("/api/v1/employees/{id}/deactivate", { params: { path: { id: u.id } } }))),
      }}
      onDelete={capabilities.has("staff.delete") ? async (u, urgent) => void (await call(api.DELETE("/api/v1/employees/{id}", { params: { path: { id: u.id } }, keepalive: urgent }))) : null}
      panel={({ row, saved }) => {
        const job = row ? employmentOf(row) : null;
        const jobInitial = {
          employment_type: job?.employment_type ?? "permanent",
          engaged_from: job?.engaged_from ?? "",
          engaged_until: job?.engaged_until ?? "",
          project_ids: (job?.project_ids ?? []).join(","),
        };
        /**
         * The employment fields (demo plan §1), sent only when one changed: a temporary worker's dates
         * and projects; permanent clears them on the API's side.
         */
        const employment = (v: Record<string, string>): Record<string, unknown> => {
          const changed = (Object.keys(jobInitial) as (keyof typeof jobInitial)[]).some((k) => (v[k] ?? "") !== jobInitial[k]);
          if (!changed) return {};
          if (!temporary(v)) return { employment_type: "permanent" };
          return { employment_type: "temporary", engaged_from: v.engaged_from, engaged_until: v.engaged_until, project_ids: (v.project_ids ?? "").split(",").filter(Boolean) };
        };
        return (
        <RecordForm
          key={row?.id ?? "new"}
          existing={row !== null}
          fields={fields(row !== null)}
          initial={{
            full_name: row?.full_name ?? "",
            email: row?.email ?? "",
            role: row?.role ?? "employee",
            phone: row?.phone ?? "",
            default_hourly_rate: row?.default_hourly_rate ? moneyText(row.default_hourly_rate) : "",
            ...jobInitial,
          }}
          onSave={async (v) => {
            const role = v.role as (typeof roles)[number];
            // The rate only when it changed: a Manager is not shown another person's rate, and sending
            // its empty field would refuse the whole edit (users.rate_admin_only; CodeRabbit, PR 20).
            const before = row?.default_hourly_rate ? moneyText(row.default_hourly_rate) : "";
            const rate = (v.default_hourly_rate ?? "") !== before ? { default_hourly_rate: orNull(v.default_hourly_rate) } : {};
            if (row) {
              await call(api.PATCH("/api/v1/employees/{id}", { params: { path: { id: row.id } }, body: { full_name: v.full_name!.trim(), role, phone: orNull(v.phone), ...rate, ...employment(v) } }));
              await saved(row.id);
            } else {
              const made = await call(api.POST("/api/v1/employees", { body: { full_name: v.full_name!.trim(), email: v.email!.trim(), role, phone: orNull(v.phone), ...rate, ...employment(v), initial_password: v.initial_password ?? "" } }));
              await saved(made.id);
            }
          }}
        />
        );
      }}
    />
    </>
  );
}

const styles = StyleSheet.create({
  chips: { flexDirection: "row", flexWrap: "wrap", gap: 4 },
  nameCell: { flexDirection: "row", alignItems: "center", gap: 10, minWidth: 0 },
  avatar: { width: 30, height: 30, borderWidth: 1, alignItems: "center", justifyContent: "center" },
  name: { flexShrink: 1, minWidth: 0 },
});
