/**
 * Above everything on the web (the owner, 2026-11-13): a floating card drawn into the page's body,
 * so no scroll area clips it and no side menu covers it, and `position: fixed` means the window
 * again. It keeps the app's direction, so its text reads as the screen does.
 */
import type { ReactNode } from "react";
import { createPortal } from "react-dom";
import { View } from "react-native";

import { useDirectionProps } from "./Screen";

export function Portal({ children }: { children: ReactNode }) {
  const direction = useDirectionProps();
  if (typeof document === "undefined") return <>{children}</>;
  return createPortal(
    <View {...direction.web} style={direction.style}>
      {children}
    </View>,
    document.body,
  );
}
