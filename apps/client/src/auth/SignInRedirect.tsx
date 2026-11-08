/**
 * Send a signed-out visitor to sign-in and keep where they were going (contracts/ui-routes.md,
 * "Deep links"): the address, with its query, rides along as `next`; the language rides along on
 * its own so the sign-in screen is already in it. A preview address in a build without previews is
 * no one's page, so it shows the 404 page instead (FR-068).
 */
import { Redirect, useGlobalSearchParams, usePathname } from "expo-router";

import { isPreviewPath, previewsEnabled } from "../config/previews";
import { NotFoundScreen } from "../screens/misc/NotFoundScreen";
import { encodeReturnPath } from "./returnPath";

/** Redirect to sign-in, remembering the current address. */
export function SignInRedirect() {
  const pathname = usePathname();
  const params = useGlobalSearchParams<Record<string, string | string[]>>();
  if (!previewsEnabled && isPreviewPath(pathname)) return <NotFoundScreen />;
  const next = encodeReturnPath(pathname, params);
  const lang = typeof params.lang === "string" ? `&lang=${encodeURIComponent(params.lang)}` : "";
  return <Redirect href={`/sign-in?next=${next}${lang}` as never} />;
}
