# ADR 014: HDS draws its own components on React Native's primitives

**Status**: Accepted, 2026-11-07 (specs/007-ux-redesign, group A, task T014; research R1 and R2)

## Context

Feature 007 redraws every screen in Option B (docs/plans/ux-working-plan.md section 3): square
corners, one clipped corner on feature panels and the primary action, 1 px status chips, a 2 px
active rule, IBM Plex Sans and IBM Plex Sans Hebrew, four themes generated from DTCG tokens, Hebrew
right to left first. The plan shortlisted a Uniwind kit (React Native Reusables or HeroUI Native),
Tamagui, or Unistyles with HDS's own components, and two date pickers (react-native-ui-datepicker,
react-native-paper-dates), to be chosen by a Hebrew RTL spike.

### What the spike could and could not check

The machine has no Node on its PATH, no Xcode and an Android SDK with one emulator image. The spike
ran:

- **Web**: each candidate installed into a throwaway copy of the app, a probe screen with a list, a
  form, tabs, a select, a switch, a bottom sheet and a range picker, exported with `expo export`,
  measured by Playwright at 390 × 844 in Hebrew and English (positions of the first and last item of a
  row, the tabs, the select's chevron, the switch, the resolved start padding, the input's text
  alignment, the sheet).
- **iOS**: a development build (`expo run:ios`) on the iPhone 16 simulator, iOS 18.3, with the
  device in Hebrew, once the owner had installed Xcode and Node. The same probe screens were opened
  there and screenshotted.
- **Android**: the emulator (API 36.1) with Expo Go 58 rendered the React Native primitives probe in
  English. Expo Go cannot switch layout direction (`I18nManager.forceRTL` needs a development
  build), and an Android development build was not attempted tonight (Gradle against the installed
  JDK 25 is an unknown). **Android RTL is therefore not verified on a device**; it relies on the
  same React Native mechanism the iOS build verified.

### Results

| Candidate | Web result | Native RTL evidence | Option B fit |
| --- | --- | --- | --- |
| React Native primitives (own components) | Rows, tabs, the select's chevron, the switch and text alignment all mirror under `dir="rtl"`. `paddingStart` and the other logical styles resolve per element from React Native Web's `LocaleProvider`, which a `dir` prop on the root View provides (confirmed in `react-native-web/dist/exports/createElement`); without it they fall back to left to right. | **Verified on the Hebrew iOS build**: rows, tabs, the select's chevron, the switch, `paddingStart` and the range picker all mirror. Rendered on the Android emulator in English only. | Exact: nothing to override. |
| Tamagui 3.2.3 (`@tamagui/config` v5 and v6) | Did not start with either default config: the production bundle reports error 008, the development build names the cause, "Must set animations in tamagui.config.ts". With `@tamagui/config/animations-rn` the web refuses ("this driver only works on native"), so each platform needs its own driver. | With the native driver the iOS probe crashed on an undefined component (`Element type is invalid`). Open RTL issues; no logical-property story in its docs. Not pursued further. | Rounded, Material-like defaults; everything would be overridden. |
| HeroUI Native 1.0.11 on Uniwind 1.13.2 | Builds for the web once nine peer packages are pinned (Nitro Modules, Reanimated, Worklets, Gesture Handler, Bottom Sheet, Blur, tailwind-variants 3, tailwind-merge, Tailwind 4). Tailwind's logical utilities (`ps-10`) resolve to `padding-inline-start` and mirror correctly. | **Renders and mirrors on the Hebrew iOS build** (`flex-row`, `ps-10`); needs a development build (Nitro). Uniwind documents RTL variants; HeroUI exposes `useIsRTL`. A credible later option. | shadcn-style rounded shapes; the clipped corner and 1 px chips would be custom anyway. |
| Unistyles 3.8.1 with own components | Not run: needs Nitro Modules, a development build and a Babel plugin. Its wins (themes without re-render, breakpoints, variants) are small at HDS's size (tens of users, about 35 screens). | Same as React Native's. | Same as own components. |
| react-native-ui-datepicker 3.3.0 | Range mode renders Sunday-first weeks with Hebrew month and weekday names and mirrors on the web. No keyboard grid, no typed entry, no presets, no `gridcell` roles. | Mirrors on the Hebrew iOS build (headers, month arrows). None documented. | Unstyled; a wrapper for everything the spec asks for. |
| react-native-paper-dates 0.24.1 | Range modal renders right to left on the web, Material look, needs react-native-paper's provider; its header swaps the two dates in Hebrew. | Renders right to left on the Hebrew iOS build too. Documented Hebrew and RTL. | Material; cannot draw Option B. |

## Decision

1. **HDS's own components on React Native's primitives** (`View`, `Text`, `Pressable`, `TextInput`,
   `Modal`, `ScrollView`), styled with `StyleSheet` from a theme context that the generator
   (`apps/client/scripts/build-theme.ts`) builds from the DTCG tokens. The root View carries
   `dir`, so React Native Web resolves every start/end style for the language.
2. **Motion** with React Native's `Animated` on the native driver (transform and opacity), reduced
   to zero duration under the device's reduce-motion setting; no third-party animation library in
   007. Reanimated can replace it later behind the same `src/design/motion/` API if a screen needs
   the UI thread for gestures.
3. **Icons and clipped corners** with `react-native-svg`; arrows mirror through a transform in
   Hebrew, the clock icon never does (O13).
4. **Fonts** through `expo-font` and the `@expo-google-fonts/ibm-plex-sans` and
   `@expo-google-fonts/ibm-plex-sans-hebrew` packages (400, 500, 600).
5. **Date and range pickers are HDS's own** (`src/design/components/date/`), built on
   `src/locale/plainDate.ts`: typed entry with any separator, the W3C grid keyboard pattern,
   presets, Sunday weeks, Israeli holidays, two months side by side. Neither shortlisted picker
   gives the keyboard, typed-entry or preset behaviour the spec requires, so a wrapper would have
   rebuilt most of it.

## Consequences

- No native module is added for styling, so Expo Go keeps working for development and the manual
  screen-reader passes (group H).
- Every component is HDS's to maintain; the gallery (`/dev/gallery`) and the React Native Testing
  Library tests are what keep them honest.
- Android RTL is verified on a device in group H (FR-065); iOS was verified by the spike, and
  both rely only on React Native's own direction handling.
- Revisit if a later feature needs gesture-driven motion (Reanimated) or if HeroUI Native gains a
  square theme worth adopting.
- Found while building the shells (group B):
  - expo-router's `Link asChild` merges its child's style as a plain object, so a `Pressable`
    style function renders unstyled on the web. `LinkPressable`
    (`apps/client/src/design/components/LinkPressable.tsx`) keeps the press state itself and
    hands the link one flat style; a unit test reproduces the merge.
  - The web build turns expo-router's async routes off (`asyncRoutes: false` in `app.json`). With
    them on (SDK 58's default on the web), each route sat behind a lazy import and React held each
    Suspense retry for 300 ms, so a cold load took about 650 ms to the sign-in button. One bundle
    (about 3.9 MB, against 4 MB in 56 chunks) shows it at about 75 ms. Revisit if the bundle grows
    enough that splitting pays for itself.
