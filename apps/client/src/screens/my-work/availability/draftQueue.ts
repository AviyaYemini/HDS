/**
 * The device draft (specs/007-ux-redesign FR-019): a cell change the server could not be reached
 * for is kept on the device, shown as made, and sent when the connection is back. One list per
 * person and window, in async storage, in the order the changes were made.
 */
import AsyncStorage from "@react-native-async-storage/async-storage";

import type { CellKey, CellState } from "./model";

export interface PendingChange {
  key: CellKey;
  state: CellState;
}

function storageKey(userId: string, windowId: string): string {
  return `hds.availability.pending.${userId}.${windowId}`;
}

export async function loadPending(userId: string, windowId: string): Promise<PendingChange[]> {
  try {
    const raw = await AsyncStorage.getItem(storageKey(userId, windowId));
    if (!raw) return [];
    const parsed: unknown = JSON.parse(raw);
    return Array.isArray(parsed) ? (parsed as PendingChange[]) : [];
  } catch {
    return [];
  }
}

export async function savePending(userId: string, windowId: string, pending: readonly PendingChange[]): Promise<void> {
  try {
    if (pending.length === 0) await AsyncStorage.removeItem(storageKey(userId, windowId));
    else await AsyncStorage.setItem(storageKey(userId, windowId), JSON.stringify(pending));
  } catch {
    // Storage refused: the draft lives in memory until the screen closes.
  }
}

/** `pending` with `change` added, replacing an earlier change to the same cell. */
export function withChange(pending: readonly PendingChange[], change: PendingChange): PendingChange[] {
  const same = (a: CellKey, b: CellKey) => a.date === b.date && a.shiftType === b.shiftType;
  return [...pending.filter((item) => !same(item.key, change.key)), change];
}
