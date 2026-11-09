/**
 * A proposal's review state and its writes (specs/008-auto-scheduler-review FR-003 to FR-016, FR-033):
 * the working copy as read or as the last edit answered, the pairs chosen, the open panels, "who else
 * could cover" for the slot being assigned to, and accept, discard, edit and undo. Every write sends
 * the version shown; a stale one reads the proposal again.
 */
import { useRouter } from "expo-router";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import { call, codeOf } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { showToast } from "../../design/components";
import { startOfWeek } from "../../locale";
import { rosterHref } from "../roster/ProjectChooser";
import { useLoad } from "../useLoad";
import { projectsOf, type Job } from "./model";
import { breaksOf, pairsOf, type AcceptPart, type Cover, type Edit, type EditWarning, type Pair, type Review, type ReviewSlot } from "./review";

export interface PersonOn {
  slot: ReviewSlot;
  userId: string;
}

export type Pending = { edit: Edit; breaks: EditWarning[] };

export function useProposalReview(job: Job, onDecided: () => void, onVersion?: (version: number) => void) {
  const { t } = useTranslation();
  const session = useSession();
  const router = useRouter();
  const loaded = useLoad(
    async () => call(session.api.GET("/api/v1/schedule-jobs/{id}/proposal", { params: { path: { id: job.id } } })),
    [job.id, job.proposal_state],
  );
  const [review, setReview] = useState<Review | null>(null);
  const shown = review ?? loaded.data ?? null;
  const [busy, setBusy] = useState<string | null>(null);
  const [problem, setProblem] = useState<string | null>(null);
  const [chosen, setChosen] = useState<Pair[]>([]);
  const [personOn, setPersonOn] = useState<PersonOn | null>(null);
  const [assignTo, setAssignTo] = useState<ReviewSlot | null>(null);
  const [moving, setMoving] = useState<{ from: PersonOn; swap: boolean } | null>(null);
  const [pending, setPending] = useState<Pending | null>(null);
  const [reason, setReason] = useState("");
  // Who else could cover the slot being assigned to (FR-014): read each time the sheet opens on a
  // slot, since the working copy changes with every edit.
  const [fetched, setFetched] = useState<{ key: string; version: number; attempt: number; data: Cover | null; error: unknown } | null>(null);
  const [attempt, setAttempt] = useState(0);
  const shownVersion = review?.version ?? loaded.data?.version ?? -1;
  useEffect(() => {
    if (shownVersion >= 0) onVersion?.(shownVersion);
  }, [shownVersion, onVersion]);
  useEffect(() => {
    if (assignTo === null) return;
    let live = true;
    call(session.api.GET("/api/v1/schedule-jobs/{id}/slots/{key}/cover", { params: { path: { id: job.id, key: assignTo.key } } }))
      .then((data) => live && setFetched({ key: assignTo.key, version: shownVersion, attempt, data, error: null }))
      .catch((error: unknown) => live && setFetched({ key: assignTo.key, version: shownVersion, attempt, data: null, error }));
    return () => {
      live = false;
    };
  }, [assignTo, job.id, session.api, shownVersion, attempt]);
  // The answer for the slot open now, read on the version shown; anything else is still loading.
  const cover = assignTo && fetched?.key === assignTo.key && fetched.version === shownVersion && fetched.attempt === attempt ? fetched : null;
  const version = `"${shown?.version ?? -1}"`;

  const stale = (code: string) => {
    if (code === "schedule.version_stale") {
      setReview(null);
      loaded.reload();
    }
  };

  const accept = async (part: AcceptPart | null, label: string) => {
    setBusy(label);
    setProblem(null);
    try {
      const answer = await call(
        session.api.POST("/api/v1/schedule-jobs/{id}/accept", {
          params: { path: { id: job.id }, header: { "If-Match": version } },
          ...(part ? { body: part } : {}),
        }),
      );
      // The roster week accepted is the next step, where Publish is one tap away (FR-033): all
      // of it goes there at once; a part offers it, so the rest can still be reviewed.
      // The project accepted, when the part names one; else the run's first.
      const project = (part && "project_ids" in part && part.project_ids?.[0]) || projectsOf(job)[0];
      const week = startOfWeek(part && "days" in part && part.days?.[0] ? part.days[0] : job.from);
      const roster = project ? rosterHref(project, week) : null;
      showToast(t("runs.accepted", { count: answer.shifts_created + answer.shifts_updated }), {
        tone: "success",
        ...(roster && answer.state !== "accepted" ? { action: { label: t("runs.open_roster"), onPress: () => router.push(roster as never) } } : {}),
      });
      setChosen([]);
      setReview(null);
      loaded.reload();
      onDecided();
      if (roster && answer.state === "accepted") router.push(roster as never);
    } catch (failure) {
      setProblem(codeOf(failure));
      stale(codeOf(failure));
    } finally {
      setBusy(null);
    }
  };

  const discardRest = async () => {
    setBusy("discard");
    setProblem(null);
    try {
      await call(session.api.POST("/api/v1/schedule-jobs/{id}/discard", { params: { path: { id: job.id }, header: { "If-Match": version } } }));
      setReview(null);
      loaded.reload();
      onDecided();
    } catch (failure) {
      setProblem(codeOf(failure));
      stale(codeOf(failure));
    } finally {
      setBusy(null);
    }
  };

  const closePanels = () => {
    setPersonOn(null);
    setAssignTo(null);
    setMoving(null);
    setPending(null);
    setReason("");
  };

  const send = async (edit: Edit) => {
    setBusy("edit");
    setProblem(null);
    let outcome;
    try {
      outcome = await session.api.POST("/api/v1/schedule-jobs/{id}/edits", {
        params: { path: { id: job.id }, header: { "If-Match": version } },
        body: edit,
      });
    } catch {
      // No answer at all (the network): say so, and leave every button usable (R7).
      closePanels();
      setProblem("errors.network");
      return;
    } finally {
      setBusy(null);
    }
    if (outcome.response.ok && outcome.data) {
      setReview(outcome.data.proposal);
      closePanels();
      const kept = outcome.data.warnings.filter((w) => !w.excused && w.code === "warnings.close_preference");
      if (kept.length > 0) showToast(t("warnings.close_preference"), { tone: "warning" });
      return;
    }
    const code = codeOf(outcome.error);
    if (code === "schedule.override_reason_required") {
      // The same sheet turns to the reason: one sheet open at a time, never one closing under another.
      setPending({ edit, breaks: breaksOf(outcome.error) });
      return;
    }
    closePanels();
    setProblem(code);
    stale(code);
  };

  const undo = async () => {
    setBusy("undo");
    setProblem(null);
    try {
      setReview(await call(session.api.POST("/api/v1/schedule-jobs/{id}/edits/undo", { params: { path: { id: job.id }, header: { "If-Match": version } } })));
    } catch (failure) {
      setProblem(codeOf(failure));
      stale(codeOf(failure));
    } finally {
      setBusy(null);
    }
  };

  const toggle = (slot: ReviewSlot, mark: ReviewSlot["marks"][number]) => {
    const pairs = pairsOf(slot, mark);
    const has = chosen.some((p) => p.key === slot.key && p.userId === mark.user_id);
    setChosen((now) => (has ? now.filter((p) => !(p.key === slot.key && pairs.some((q) => q.userId === p.userId))) : [...now, ...pairs]));
  };


  return {
    loaded,
    shown,
    busy,
    problem,
    chosen,
    personOn,
    setPersonOn,
    assignTo,
    setAssignTo,
    moving,
    setMoving,
    pending,
    reason,
    setReason,
    setAttempt,
    cover,
    accept,
    discardRest,
    closePanels,
    send,
    undo,
    toggle,
  };
}
