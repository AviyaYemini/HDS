/**
 * Send a cell change's planned calls in order (specs/007-ux-redesign FR-015; R49, R50): create,
 * update or delete the person's availability entries, as model.planCellChange planned them.
 */
import type { ApiClient } from "@hds/api-client";

import { call } from "../../../api/errors";
import type { Mark, PlannedCall } from "./model";

export async function runPlan(api: ApiClient, userId: string, calls: readonly PlannedCall[]): Promise<void> {
  for (const step of calls) {
    if (step.call === "create") {
      await call(
        api.POST("/api/v1/employees/{id}/constraints", {
          params: { path: { id: userId } },
          body: { kind: step.kind, starts_on: step.starts_on, ends_on: step.ends_on, shift_type_id: step.shift_type_id, weekday: step.weekday },
        }),
      );
    } else if (step.call === "update") {
      const body: { kind?: Mark; starts_on?: string; ends_on?: string } = {};
      if (step.kind) body.kind = step.kind;
      if (step.starts_on) body.starts_on = step.starts_on;
      if (step.ends_on) body.ends_on = step.ends_on;
      await call(api.PATCH("/api/v1/constraints/{id}", { params: { path: { id: step.id } }, body }));
    } else {
      await call(api.DELETE("/api/v1/constraints/{id}", { params: { path: { id: step.id } } }));
    }
  }
}
