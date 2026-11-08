/**
 * Retry on return (PR 2 review): while the app waits for the server, a retry runs on its own when
 * the person comes back, that is when the app returns to the foreground on a phone or the browser
 * reports the network back on the web. No timer: the person's return is the signal.
 */
import { useEffect } from "react";
import { AppState, Platform } from "react-native";

export function useRetryOnReturn(active: boolean, retry: () => void): void {
  useEffect(() => {
    if (!active) return;
    if (Platform.OS === "web") {
      if (typeof window === "undefined") return;
      window.addEventListener("online", retry);
      return () => window.removeEventListener("online", retry);
    }
    const subscription = AppState.addEventListener("change", (state) => {
      if (state === "active") retry();
    });
    return () => subscription.remove();
  }, [active, retry]);
}
