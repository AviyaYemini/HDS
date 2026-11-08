/**
 * The previews switch (specs/007-ux-redesign research R11, FR-067 to FR-069).
 *
 * `EXPO_PUBLIC_PREVIEWS=1` at build time turns on the development gallery and the preview stubs of
 * later features. Expo inlines `EXPO_PUBLIC_` variables into the bundle, so a production build made
 * without it does not render them at all. It is on in development and the shared test environment,
 * and never set for production.
 */
export const previewsEnabled: boolean = process.env.EXPO_PUBLIC_PREVIEWS === "1";
