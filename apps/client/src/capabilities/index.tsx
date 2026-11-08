/**
 * What the signed-in person may do, from `GET /me` (specs/007-ux-redesign FR-001, FR-007, T032).
 *
 * The API serves the capability names from the same map its route gates ask
 * (`apps/api/src/hds/core/capabilities.py`), so navigation drawn from here and the API's refusals
 * cannot disagree. Screens never test a role: they ask for a capability. A name ending in `:project`
 * holds only on the projects in `managed_project_ids`, or on every project for an Admin, exactly as
 * the API's `has_on_project` decides.
 */
import type { components } from "@hds/api-client";
import { Redirect } from "expo-router";
import { useSyncExternalStore, type ReactNode } from "react";

import { useSession } from "../api/SessionContext";
import type { Me } from "../api/session";

/** One capability name, as the API's contract lists them. */
export type Capability = components["schemas"]["Capability"];

/** What one person may do. */
export interface Capabilities {
  /** Every capability held, scoped ones included. */
  readonly names: ReadonlySet<Capability>;
  /** The projects a Manager manages; empty for every other role. */
  readonly managedProjectIds: ReadonlySet<string>;
  /** Whether `capability` is held at all (for a scoped one, on some project). */
  has(capability: Capability): boolean;
  /** Whether `capability` holds on `projectId`; an unscoped one falls back to {@link has}. */
  hasOnProject(capability: Capability, projectId: string): boolean;
}

/** Whether `capability` is scoped to projects. */
export function isScoped(capability: Capability): boolean {
  return capability.endsWith(":project");
}

/** The capabilities of `me`; nothing at all when signed out. */
export function capabilitiesOf(me: Me | null): Capabilities {
  const names = new Set<Capability>(me?.capabilities ?? []);
  const managed = new Set<string>(me?.managed_project_ids ?? []);
  const admin = me?.role === "admin";
  return {
    names,
    managedProjectIds: managed,
    has: (capability) => names.has(capability),
    hasOnProject: (capability, projectId) => {
      if (!names.has(capability)) return false;
      if (!isScoped(capability) || admin) return true;
      return managed.has(projectId);
    },
  };
}

/** The signed-in person's capabilities, redrawn when the account is read again. */
export function useCapabilities(): Capabilities {
  const session = useSession();
  const me = useSyncExternalStore(session.subscribe, () => session.user);
  return capabilitiesOf(me);
}

interface Need {
  /** The capability asked for. */
  need: Capability;
  /** For a `:project` capability, the project it must hold on. */
  project?: string;
}

function allowed(capabilities: Capabilities, { need, project }: Need): boolean {
  return project === undefined ? capabilities.has(need) : capabilities.hasOnProject(need, project);
}

/**
 * `children` only when the capability is held; otherwise `fallback` (nothing by default). For an
 * action that is only blocked for now, show it disabled with the reason instead (FR-008).
 */
export function Gate({ need, project, children, fallback = null }: Need & { children: ReactNode; fallback?: ReactNode }) {
  const capabilities = useCapabilities();
  return <>{allowed(capabilities, { need, project }) ? children : fallback}</>;
}

/**
 * A route's guard: the screen when the capability is held, else the no-access page (FR-007). The
 * page is `denied` when given; by default the person is sent to `/no-access`, which offers the way
 * home.
 */
export function RequireCapability({
  need,
  project,
  children,
  denied,
}: Need & { children: ReactNode; denied?: ReactNode }) {
  const capabilities = useCapabilities();
  if (allowed(capabilities, { need, project })) return <>{children}</>;
  return <>{denied ?? <Redirect href="/no-access" />}</>;
}
