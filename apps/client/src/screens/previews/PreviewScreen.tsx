/**
 * Preview stubs of later features (specs/007-ux-redesign FR-068, FR-069, US8; the working plan's
 * section 6). Each is an Option B empty state that says what the screen will do and which feature
 * brings it, with nothing that pretends to work. Shown only while previews are switched on; with
 * them off the address is unknown (the 404 page).
 */
import { useTranslation } from "react-i18next";

import { type Capability, RequireCapability } from "../../capabilities";
import { previewsEnabled } from "../../config/previews";
import { EmptyState, PageTitle } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import type { IconName } from "../../design/icons/Icon";
import { NoAccessScreen } from "../no-access/NoAccessScreen";
import { NotFoundScreen } from "../misc/NotFoundScreen";

/** The previews with their own address (contracts/ui-routes.md). */
export const PREVIEWS = {
  billing: { feature: "009", icon: "wallet", need: ["client_portal.read"], eyebrowKey: "nav.group.client" },
  requests: { feature: "009", icon: "mail", need: ["client_portal.read"], eyebrowKey: "nav.group.client" },
  "client-requests": { feature: "009", icon: "mail", need: ["client_requests.answer"], eyebrowKey: "nav.group.manage" },
  overview: { feature: "012", icon: "chart", need: ["roster.read"], eyebrowKey: "nav.group.manage" },
  approvals: { feature: "016", icon: "check", need: ["client_requests.answer"], eyebrowKey: "nav.group.manage" },
  "absence-requests": { feature: "016", icon: "calendar", need: ["my_work.read"], eyebrowKey: "nav.group.my_work" },
  inbox: { feature: "010", icon: "inbox", need: ["account.self"], eyebrowKey: "nav.inbox" },
  "notification-prefs": { feature: "010", icon: "settings", need: ["account.self"], eyebrowKey: "nav.account" },
  "audit-log": { feature: "014", icon: "list", need: ["staff.delete"], eyebrowKey: "nav.group.admin" },
} as const satisfies Record<string, { feature: string; icon: IconName; need: readonly Capability[]; eyebrowKey: string }>;

export type PreviewKey = keyof typeof PREVIEWS;

/** The test id a preview carries: `preview-<key>`, with the manager's requests sharing the client's id. */
export function previewTestId(key: PreviewKey): string {
  return key === "client-requests" ? "preview-requests" : `preview-${key}`;
}

/** The stub for `id`, gated like any other screen. */
export function PreviewScreen({ id }: { id: PreviewKey }) {
  const { t } = useTranslation();
  const preview = PREVIEWS[id];
  if (!previewsEnabled) return <NotFoundScreen />;
  const need = preview.need[0]!;
  return (
    <RequireCapability need={need} denied={<NoAccessScreen />}>
      <Screen testID={`screen-${previewTestId(id)}`}>
        <PageTitle eyebrow={[t("app.name"), t(preview.eyebrowKey)]} title={t(`preview.${id}.title`)} />
        <EmptyState
          testID={previewTestId(id)}
          badge={t("preview.badge")}
          icon={preview.icon}
          title={t(`preview.${id}.title`)}
          body={t(`preview.${id}.body`)}
          feature={t("preview.comes_with", { feature: preview.feature })}
        />
      </Screen>
    </RequireCapability>
  );
}
