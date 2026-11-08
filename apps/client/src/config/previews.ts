/**
 * The previews switch (specs/007-ux-redesign research R11, FR-067 to FR-069).
 *
 * `EXPO_PUBLIC_PREVIEWS=1` at build time turns on the development gallery and the preview stubs of
 * later features. Expo inlines `EXPO_PUBLIC_` variables into the bundle, so a production build made
 * without it does not render them at all. It is on in development and the shared test environment,
 * and never set for production.
 */
export const previewsEnabled: boolean = process.env.EXPO_PUBLIC_PREVIEWS === "1";

/**
 * The addresses that exist only as previews (FR-068; contracts/ui-routes.md "Preview stubs"). The
 * route files are always in the bundle, so a build made without previews treats them as unknown
 * addresses: the 404 page, signed in or not, instead of a sign-in redirect or a stub.
 */
export const PREVIEW_PATHS: readonly string[] = [
  "/client/billing",
  "/client/requests",
  "/manage/requests",
  "/manage/overview",
  "/manage/approvals",
  "/my/requests",
  "/inbox",
  "/account/notifications",
  "/admin/audit-log",
  "/dev/gallery",
];

/** Whether `pathname` is an address that only a build with previews knows. */
export function isPreviewPath(pathname: string): boolean {
  return PREVIEW_PATHS.some((path) => pathname === path || pathname.startsWith(`${path}/`));
}
