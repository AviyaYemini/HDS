/**
 * Send a signed-out visitor to sign-in and keep where they were going (contracts/ui-routes.md,
 * "Deep links"): the address, with its query, rides along as `next`; the language rides along on
 * its own so the sign-in screen is already in it. The router's global params also carry the route's
 * dynamic segments (`[id]` for /my/shifts/abc gives `id=abc`), which are already in the path, so
 * those and `lang` are left out of the rebuilt query. A preview address in a build without previews
 * is no one's page, so it shows the 404 page instead (FR-068).
 */
import { Redirect, useGlobalSearchParams, usePathname, useSegments } from "expo-router";
import { useEffect } from "react";

import { isPreviewPath, previewsEnabled } from "../config/previews";
import { NotFoundScreen } from "../screens/misc/NotFoundScreen";
import { encodeReturnPath } from "./returnPath";

/** The dynamic segment names in `segments`: `[id]` and `[...rest]` give `id` and `rest`. */
function segmentNames(segments: readonly string[]): Set<string> {
  const names = new Set<string>();
  for (const segment of segments) {
    const match = /^\[(?:\.\.\.)?(.+)\]$/.exec(segment);
    if (match?.[1]) names.add(match[1]);
  }
  return names;
}

/** `params` without the route's own segments and without `lang`, which rides along on its own. */
export function queryOnly(params: Record<string, string | string[] | undefined>, segments: readonly string[]): Record<string, string | string[] | undefined> {
  const drop = segmentNames(segments);
  drop.add("lang");
  return Object.fromEntries(Object.entries(params).filter(([key]) => !drop.has(key)));
}

/**
 * The address the shell last showed while signed in. When a session ends on a page, the section's
 * layout stops rendering its routes, and the router rewrites the address to the section's first
 * route (/home) before the redirect reads it, keeping the old page's params (`/home?id=…`). So the
 * shell remembers where it was, and the redirect prefers that (the owner, 2026-11-13).
 */
let remembered: string | null = null;

/** Keep the shown address while signed in (the shells call it). */
export function useRememberLocation(): void {
  const pathname = usePathname();
  const params = useGlobalSearchParams<Record<string, string | string[]>>();
  const segments = useSegments() as string[];
  const next = encodeReturnPath(pathname, queryOnly(params, segments));
  useEffect(() => {
    remembered = next;
  }, [next]);
}

/** Forget the remembered address: a sign-out or a new sign-in starts afresh. */
export function forgetLocation(): void {
  remembered = null;
}

/** Redirect to sign-in, back to where the person was. */
export function SignInRedirect() {
  const pathname = usePathname();
  const params = useGlobalSearchParams<Record<string, string | string[]>>();
  const segments = useSegments() as string[];
  if (!previewsEnabled && isPreviewPath(pathname)) return <NotFoundScreen />;
  const next = remembered ?? encodeReturnPath(pathname, queryOnly(params, segments));
  const lang = typeof params.lang === "string" ? `&lang=${encodeURIComponent(params.lang)}` : "";
  return <Redirect href={`/sign-in?next=${next}${lang}` as never} />;
}
