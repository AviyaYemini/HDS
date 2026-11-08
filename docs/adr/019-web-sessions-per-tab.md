# ADR 019: The web keeps its session per tab

**Date**: 2026-11-08
**Status**: Accepted. Amends [ADR 010](010-client-stack.md), which kept the web's tokens in memory only.

## Context

ADR 010 kept the web client's access and refresh tokens in memory, so no script injected into the
page could read a stored refresh token; the price was that every reload, bookmark or link opened
in a new page asked for the password again. 007 FR-010 says web sessions keep their 7 days, which
memory alone never delivered beyond one page load, and the deep links of FR-007 then always passed
through sign-in. The owner chose "per tab" on 2026-11-08: a session survives a reload or a deep link
in the same tab and ends when the tab closes.

Browsers copy a tab's `sessionStorage` into a duplicated tab, and some do when a link opens a new
tab. Two tabs would then hold the same refresh token: the first refresh rotates it, the second
presents the rotated one, and after `REFRESH_REUSE_GRACE_SECONDS` the API treats that as theft and
ends every session the person has (ADR 006).

## Decision

The web token store (`apps/client/src/api/tokens.ts`, `tabStore`) keeps the tokens in memory while
the page runs, and in the tab's `sessionStorage` only between a `pagehide` and the next load:

- on load it reads the tab's copy and removes it at once;
- it writes the current tokens back only on `pagehide` (a reload, a navigation away, closing the tab);
- a page restored from the back-forward cache removes that copy again on `pageshow`;
- signing out clears both.

So a tab duplicated, or opened with a copy of the storage, while the page is open finds nothing and
signs in again, and two tabs never present the same refresh token. A crash before `pagehide` only
means signing in again. The 7-day rolling limit (FR-010) and rotation are unchanged. Phones keep
their secure store (ADR 016).

## Consequences

- A reload, a deep link and the browser's back and forward keep the person signed in, in that tab.
- The trade-off: a script running on the page could read the stored tokens between a `pagehide` and
  the next load, where before it could only read them in memory while the page ran. Both need a
  script on the page in the first place; the window is short and ends when the tab closes.
- Each tab is its own session (shown separately under Account → Sessions); closing a tab does not
  revoke its refresh token on the server, which then expires on its own.
- *(Proposed)* A Content-Security-Policy on the web server (none today) would narrow the injected-
  script risk further; the splash screen's inline script would need a hash or a file. A follow-up,
  not part of 007.

## Alternatives considered

- **`localStorage`**: survives closing the tab and is shared by every tab, so tabs would share and
  race one refresh token, and the token outlives the tab on a shared computer.
- **`sessionStorage` written on every change**: duplicated tabs would copy a live refresh token and
  trip the reuse detection, ending all of the person's sessions.
- **An httpOnly refresh cookie**: the safest web option, as ADR 010 noted, but it needs cookie
  issuance and CSRF protection in the API; it can still replace this store later.
