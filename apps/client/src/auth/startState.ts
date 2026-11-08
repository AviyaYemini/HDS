/**
 * What the app shows after the start-up restore (specs/007-ux-redesign FR-010; PR 2 review): the
 * routes when a session was restored or there was nothing to restore, and the "can't reach HDS"
 * state when kept tokens could not be checked because the server was out of reach. The sign-in
 * form is never the answer to a network failure, since the kept session may still be good.
 */
export type StartState = "ready" | "unreachable";

export function startAfterRestore(restored: boolean, unreachable: boolean): StartState {
  return !restored && unreachable ? "unreachable" : "ready";
}
