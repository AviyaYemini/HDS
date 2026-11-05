# ADR 010: The client is one Expo app with a generated API client, and web tokens stay in memory

**Date**: 2026-11-04
**Status**: Accepted.

## Context

Feature 003 ships the first screens: sign-in, My availability and My shifts, for employees on phones and
in a browser. docs/hds-spec.md section 7 places the app in `apps/client`. Constitution V says the client
is generated from the OpenAPI document, Constitution VIII says every string is externalised and the
layout works right to left, and Constitution VI says secrets and tokens are kept safely. The plan and
research R9 chose the stack; this record keeps the reasons.

## Decision

**One Expo app for iOS, Android and the web.** Expo Router (file routes in `apps/client/app/`),
TypeScript strict, and React Native Web for the browser build, so the three share every screen.

**The API client is generated, never written.** `openapi-typescript` turns the API's OpenAPI document
into types in `packages/api-client/src/schema.d.ts`, and `openapi-fetch` is the typed client over them:
a wrong path, body or answer does not compile. `pnpm api-client:generate` regenerates it from the API
itself; `pnpm api-client:check` fails when the committed copy differs, and CI runs the check.

**Strings through `i18next`.** `react-i18next` with one flat key file per language in
`apps/client/src/locales/`; English is complete and other languages fall back to it key by key. The
API's error codes are keys too, so a refusal is shown in the reader's language. ESLint refuses literal
text in JSX. Right to left comes from `I18nManager` on phones and the document's `dir` on the web, and
styles use start and end only.

**Where the tokens are kept.** On a phone, the access and refresh tokens go to `expo-secure-store` (the
Keychain or the Android Keystore). On the web they stay in memory only, so reloading the page signs in
again. A 401 refreshes the tokens once, shared by requests refused together, and sends the request
again; a refused refresh ends the session.

## Consequences

- A web reload asks for the password again. That is the price of keeping the refresh token away from
  scripts on the page.
- The generated file is large (about 6,000 lines) and is committed, so a contract change shows up as a
  reviewable diff in the same pull request.
- The web build is a single-page export served with a fallback to `index.html`; Expo's static
  per-route export failed with the pinned versions and was not needed.
- Hebrew client strings beyond the 001 keys are a follow-up; the layout is already tested right to left.

## Alternatives considered

- **Orval or another full SDK generator.** More generated code to review, and hooks tied to one data
  library; `openapi-fetch` is a thin typed `fetch`.
- **The web refresh token in `localStorage`.** Survives a reload, but any injected script can read it.
- **An httpOnly refresh cookie.** The safest web option, but it needs API changes (cookie issuance,
  CSRF protection) outside this feature; it can replace the in-memory store later.
- **Separate web and native apps.** Two codebases for the same three screens.
