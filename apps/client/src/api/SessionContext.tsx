/** The session for the screens below a {@link SessionProvider}. */
import { createContext, useContext, useSyncExternalStore, type ReactNode } from "react";

import type { Session } from "./session";

const SessionContext = createContext<Session | null>(null);

/** Give the screens below `session`. */
export function SessionProvider({ session, children }: { session: Session; children: ReactNode }) {
  return <SessionContext.Provider value={session}>{children}</SessionContext.Provider>;
}

/** The session; only inside a {@link SessionProvider}. */
export function useSession(): Session {
  const session = useContext(SessionContext);
  if (session === null) throw new Error("useSession needs a SessionProvider");
  return session;
}

/** Whether someone is signed in, re-rendering when that changes. */
export function useSignedIn(): boolean {
  const session = useSession();
  return useSyncExternalStore(session.subscribe, () => session.signedIn);
}
