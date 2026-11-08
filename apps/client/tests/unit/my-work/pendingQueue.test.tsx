/** The device draft's queue (FR-019; R49, R50): one sender, oldest first, each change once, backoff while out of reach. */
import { act, render } from "@testing-library/react-native";

import { setOnlineForTests } from "../../../src/connectivity";
import type { PendingChange } from "../../../src/screens/my-work/availability/draftQueue";
import { backoffMs, usePendingQueue, type PendingQueue } from "../../../src/screens/my-work/availability/usePendingQueue";

const change = (date: string, state: "open" | "close" = "open"): PendingChange => ({ key: { date, shiftType: "morning" }, state });

function Probe({ send, onRefused, expose }: { send: (c: PendingChange) => Promise<void>; onRefused: (c: PendingChange, f: unknown) => void; expose: (q: PendingQueue) => void }) {
  expose(usePendingQueue("me", "w1", send, onRefused));
  return null;
}

afterEach(() => setOnlineForTests(true));

it("waits longer after each failed attempt, up to a minute", () => {
  expect([0, 1, 2, 3, 6, 10].map(backoffMs)).toEqual([1000, 2000, 4000, 8000, 60_000, 60_000]);
});

it("sends saved changes one at a time, oldest first, once each, and drops a refused one", async () => {
  setOnlineForTests(false);
  const sent: string[] = [];
  let inFlight = 0;
  let overlap = 0;
  const send = jest.fn(async (c: PendingChange) => {
    inFlight += 1;
    if (inFlight > 1) overlap += 1;
    await new Promise((resolve) => setTimeout(resolve, 5));
    inFlight -= 1;
    if (c.key.date === "2026-11-18") throw new Error("refused");
    sent.push(c.key.date);
  });
  const onRefused = jest.fn();
  let queue: PendingQueue | null = null;
  await render(<Probe send={send} onRefused={onRefused} expose={(q) => (queue = q)} />);
  await act(async () => {
    queue!.enqueue(change("2026-11-17"));
    queue!.enqueue(change("2026-11-18"));
    queue!.enqueue(change("2026-11-19"));
    // A newer change to a cell replaces the older one in place of the list's end.
    queue!.enqueue(change("2026-11-17", "close"));
  });
  expect(send).not.toHaveBeenCalled();
  expect(queue!.pending.map((c) => `${c.key.date}:${c.state}`)).toEqual(["2026-11-18:open", "2026-11-19:open", "2026-11-17:close"]);
  await act(async () => {
    setOnlineForTests(true);
    await new Promise((resolve) => setTimeout(resolve, 60));
  });
  expect(send).toHaveBeenCalledTimes(3);
  expect(overlap).toBe(0);
  expect(sent).toEqual(["2026-11-19", "2026-11-17"]);
  expect(onRefused).toHaveBeenCalledTimes(1);
  expect(queue!.pending).toEqual([]);
});

it("keeps a change the server could not take and tries again after a pause", async () => {
  jest.useFakeTimers();
  try {
    let reachable = false;
    const send = jest.fn(async () => {
      if (!reachable) throw Object.assign(new Error("unreachable"), { retry: true });
    });
    let queue: PendingQueue | null = null;
    await render(<Probe send={send} onRefused={jest.fn()} expose={(q) => (queue = q)} />);
    await act(async () => {
      queue!.enqueue(change("2026-11-17"));
    });
    await act(async () => {
      await Promise.resolve();
    });
    expect(send).toHaveBeenCalledTimes(1);
    expect(queue!.pending).toHaveLength(1);
    await act(async () => {
      jest.advanceTimersByTime(999);
    });
    expect(send).toHaveBeenCalledTimes(1);
    reachable = true;
    await act(async () => {
      jest.advanceTimersByTime(2);
      await Promise.resolve();
    });
    expect(send).toHaveBeenCalledTimes(2);
    await act(async () => {
      await Promise.resolve();
    });
    expect(queue!.pending).toEqual([]);
  } finally {
    jest.useRealTimers();
  }
});
