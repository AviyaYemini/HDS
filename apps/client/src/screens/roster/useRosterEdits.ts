/**
 * The roster's edits (specs/007-ux-redesign FR-030 to FR-038; 008 T042d, T042f, T042g, FR-018):
 * assign, take off, move or swap, lock, and Undo for unpublished changes, each step undone only once
 * its request succeeds. Each keeps the warnings the API answers for its shifts; a stale read (412)
 * raises the banner and reads the week again.
 */
import { useState } from "react";

import { ApiError, call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { showToast } from "../../design/components";
import type { Loaded } from "../useLoad";
import type { Alert, RosterChange, Shift, Warning } from "./model";

interface Options {
  shifts: Loaded<Shift[]>;
  /** The note and who hears of it, for the edit being made now (T042g). */
  note: string;
  alert: Alert;
  /** The rule a choice made now breaks is named in a dialog that only informs (T042d). */
  onBreaks: (shift: Shift) => void;
}

export function useRosterEdits({ shifts, note, alert, onBreaks }: Options) {
  const session = useSession();
  const errorText = useErrorText();
  const [warnings, setWarnings] = useState<Map<string, Warning[]>>(() => new Map());
  const [history, setHistory] = useState<RosterChange[]>([]);
  const [stale, setStale] = useState(false);

  /** Run one roster change; a stale read (412) shows the banner and the latest version. */
  const change = async (run: () => Promise<{ shift_etag: string; warnings: Warning[] } | null>, shiftId: string): Promise<boolean> => {
    try {
      const answer = await run();
      if (answer) setWarnings((map) => new Map(map).set(shiftId, answer.warnings.filter((w) => w.shift_ids.includes(shiftId))));
      setStale(false);
      await shifts.reload();
      return true;
    } catch (failure) {
      if (failure instanceof ApiError && failure.status === 412) {
        setStale(true);
        await shifts.reload();
      } else {
        showToast(errorText(codeOf(failure)), { tone: "warning" });
      }
      return false;
    }
  };

  const assign = async (shift: Shift, userId: string, times?: { starts_at: string | null; ends_at: string | null }, record = true): Promise<string | null> => {
    let created: string | null = null;
    let breaks = false;
    const done = await change(async () => {
      const answer = await call(
        session.api.POST("/api/v1/shifts/{id}/assignments", {
          params: { path: { id: shift.id }, header: { "If-Match": shift.etag } },
          body: {
            user_id: userId,
            ...(times?.starts_at ? { starts_at: times.starts_at, ends_at: times.ends_at } : {}),
            alert: record ? alert : "none",
            ...(record && note.trim() ? { note: note.trim() } : {}),
          },
        }),
      );
      created = answer.assignment.id;
      breaks = answer.warnings.some((w) => w.employee_id === userId && w.shift_ids.includes(shift.id));
      return answer;
    }, shift.id);
    // The assignment is made; any rule it breaks is then named in a dialog that only informs
    // (T042d). Only for a choice made now, never for undo re-making an assignment.
    if (done && breaks && record) onBreaks(shift);
    if (done && record && created) setHistory((h) => [...h, { kind: "assigned", assignmentId: created!, shiftId: shift.id, userId }]);
    return done ? created : null;
  };

  /** Take someone off; the shift's new etag on success (a replace assigns with it, R40), else null. */
  const unassign = async (shift: Shift, assignmentId: string, record = true): Promise<string | null> => {
    const before = shift.assignments.find((a) => a.id === assignmentId);
    let etag: string | null = null;
    const done = await change(async () => {
      const answer = await call(
        session.api.DELETE("/api/v1/assignments/{id}", {
          params: { path: { id: assignmentId }, header: { "If-Match": shift.etag }, query: { alert: record ? alert : "none" } },
        }),
      );
      etag = answer.shift_etag;
      return answer;
    }, shift.id);
    if (done && record && before) {
      setHistory((h) => [
        ...h,
        { kind: "unassigned", shiftId: shift.id, userId: before.user_id, starts_at: before.own_times ? before.starts_at : null, ends_at: before.own_times ? before.ends_at : null },
      ]);
    }
    return done ? etag : null;
  };

  /** Lock an assignment, so later runs keep it, or unlock it (008 FR-018). */
  const lockAssignment = async (assignmentId: string, on: boolean) => {
    try {
      const path = { params: { path: { id: assignmentId } } };
      await call(on ? session.api.PUT("/api/v1/assignments/{id}/lock", path) : session.api.DELETE("/api/v1/assignments/{id}/lock", path));
      await shifts.reload();
    } catch (failure) {
      showToast(errorText(codeOf(failure)), { tone: "warning" });
    }
  };

  /** Move an assignment to ``shift``, or swap it with ``swapWith`` there, in one step (T042f). */
  const move = async (assignmentId: string, userId: string, fromShiftId: string, shift: Shift, swapWith: string | null, record = true): Promise<boolean> => {
    try {
      const answer = await call(
        session.api.POST("/api/v1/assignments/{id}/move", {
          params: { path: { id: assignmentId } },
          body: { to_shift_id: shift.id, ...(swapWith ? { swap_with: swapWith } : {}), alert: record ? alert : "none", ...(record && note.trim() ? { note: note.trim() } : {}) },
        }),
      );
      const found = answer.warnings as Warning[];
      setWarnings((map) => {
        const next = new Map(map);
        for (const id of [fromShiftId, shift.id]) next.set(id, found.filter((w) => w.shift_ids.includes(id)));
        return next;
      });
      await shifts.reload();
      if (record) {
        setHistory((h) => [...h, { kind: "moved", assignmentId, shiftId: shift.id, fromShiftId, swapWith }]);
        if (found.some((w) => w.employee_id === userId && w.shift_ids.includes(shift.id))) onBreaks(shift);
      }
      return true;
    } catch (failure) {
      showToast(errorText(codeOf(failure)), { tone: "warning" });
      return false;
    }
  };

  // A step leaves the history only once its request has succeeded; re-creating an assignment
  // gives it a new id, which earlier steps for that person then follow (R88).
  const undo = async () => {
    const last = history[history.length - 1];
    if (!last) return;
    if (last.kind === "moved") {
      // Back to the shift it came from (in any project), swapping again when it was a swap.
      const back = { id: last.fromShiftId } as Shift;
      if (await move(last.assignmentId, "", last.shiftId, back, last.swapWith, false)) setHistory((h) => h.slice(0, -1));
      return;
    }
    const shift = shifts.data?.find((s) => s.id === last.shiftId);
    if (!shift) return;
    if (last.kind === "assigned") {
      if (await unassign(shift, last.assignmentId, false)) setHistory((h) => h.slice(0, -1));
      return;
    }
    const id = await assign(shift, last.userId, { starts_at: last.starts_at, ends_at: last.ends_at }, false);
    if (id === null) return;
    setHistory((h) =>
      h.slice(0, -1).map((step) => (step.kind === "assigned" && step.shiftId === last.shiftId && step.userId === last.userId ? { ...step, assignmentId: id } : step)),
    );
  };

  return { warnings, setWarnings, history, setHistory, stale, setStale, assign, unassign, move, undo, lockAssignment };
}
