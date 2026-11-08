/**
 * The device draft's queue (specs/007-ux-redesign FR-019; R49, R50): changes the server could not
 * be reached for, kept per person and window, sent one at a time and oldest first once the device
 * is back online, with a growing pause between failed attempts. One sender runs at a time, so a
 * change is never sent twice, and the list is always updated from its latest value.
 */
import { useCallback, useEffect, useRef, useState } from "react";

import { useOnline } from "../../../connectivity";
import { loadPending, savePending, withChange, type PendingChange } from "./draftQueue";

/** Attempt `n` (from 0) waits this long before the next try, capped at a minute. */
export function backoffMs(attempt: number): number {
  return Math.min(60_000, 1000 * 2 ** attempt);
}

export interface PendingQueue {
  pending: readonly PendingChange[];
  /** Keep a change for later; a newer change to the same cell replaces the older one. */
  enqueue: (change: PendingChange) => void;
}

/**
 * `send` applies one change on the server; it throws with `retry: true` when the server could not
 * be reached (the change stays in the queue) and with anything else when the change is refused
 * (the change is dropped and `onRefused` hears about it).
 */
export function usePendingQueue(
  userId: string,
  windowId: string | null,
  send: (change: PendingChange) => Promise<void>,
  onRefused: (change: PendingChange, failure: unknown) => void,
  onSent?: () => void,
): PendingQueue {
  const online = useOnline();
  const [pending, setPending] = useState<PendingChange[]>([]);
  const queue = useRef<PendingChange[]>([]);
  const running = useRef(false);
  const attempt = useRef(0);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [tick, setTick] = useState(0);
  const latest = useRef({ send, onRefused, onSent });
  useEffect(() => {
    latest.current = { send, onRefused, onSent };
  });

  const commit = useCallback(
    (next: PendingChange[]) => {
      queue.current = next;
      setPending(next);
      if (windowId) void savePending(userId, windowId, next);
    },
    [userId, windowId],
  );

  useEffect(() => {
    if (!windowId) return;
    let live = true;
    void loadPending(userId, windowId).then((list) => {
      if (!live) return;
      queue.current = list;
      setPending(list);
    });
    return () => {
      live = false;
    };
  }, [userId, windowId]);

  const enqueue = useCallback((change: PendingChange) => commit(withChange(queue.current, change)), [commit]);

  useEffect(() => {
    if (!online || !windowId || running.current || queue.current.length === 0) return;
    running.current = true;
    let cancelled = false;
    void (async () => {
      try {
        while (!cancelled && queue.current.length > 0) {
          const [first] = queue.current;
          if (!first) break;
          try {
            await latest.current.send(first);
            attempt.current = 0;
            commit(queue.current.filter((item) => item !== first));
            latest.current.onSent?.();
          } catch (failure) {
            if ((failure as { retry?: boolean }).retry) {
              // Still out of reach: try again later, waiting longer each time.
              const wait = backoffMs(attempt.current);
              attempt.current += 1;
              timer.current = setTimeout(() => setTick((n) => n + 1), wait);
              return;
            }
            commit(queue.current.filter((item) => item !== first));
            latest.current.onRefused(first, failure);
          }
        }
      } finally {
        running.current = false;
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [online, windowId, pending.length, tick, commit]);

  useEffect(
    () => () => {
      if (timer.current) clearTimeout(timer.current);
    },
    [],
  );

  return { pending, enqueue };
}
