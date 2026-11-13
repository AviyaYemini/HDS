/**
 * Above everything (the owner, 2026-11-13): on a phone's native app a floating card stays where it
 * is drawn; the web's version (Portal.web.tsx) lifts it to the page's top layer.
 */
import type { ReactNode } from "react";

export function Portal({ children }: { children: ReactNode }) {
  return <>{children}</>;
}
