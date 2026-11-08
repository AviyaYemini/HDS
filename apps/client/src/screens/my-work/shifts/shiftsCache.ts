/**
 * My shifts offline (specs/007-ux-redesign FR-019): the last list the device loaded, with the time
 * it was loaded, so the screen still reads when the server is out of reach.
 */
import AsyncStorage from "@react-native-async-storage/async-storage";

import type { Shift } from "./model";

export interface CachedShifts {
  items: Shift[];
  /** When the list was loaded, as an instant. */
  updatedAt: string;
}

const key = (userId: string, view: string) => `hds.shifts.${userId}.${view}`;

export async function loadCached(userId: string, view: string): Promise<CachedShifts | null> {
  try {
    const raw = await AsyncStorage.getItem(key(userId, view));
    return raw ? (JSON.parse(raw) as CachedShifts) : null;
  } catch {
    return null;
  }
}

export async function saveCached(userId: string, view: string, items: Shift[], now: Date = new Date()): Promise<void> {
  try {
    await AsyncStorage.setItem(key(userId, view), JSON.stringify({ items, updatedAt: now.toISOString() } satisfies CachedShifts));
  } catch {
    // No storage: the list is read live only.
  }
}
