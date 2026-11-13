/** Every page of a range's shifts across the projects the reader sees (the sheet, staff control). */
import { call } from "../../api/errors";
import type { useSession } from "../../api/SessionContext";
import type { Shift } from "./model";

export async function allShifts(api: ReturnType<typeof useSession>["api"], from: string, to: string, status?: "draft" | "published"): Promise<Shift[]> {
  const all: Shift[] = [];
  let cursor: string | null = null;
  do {
    const query: { from: string; to: string; limit: number; cursor?: string; status?: "draft" | "published" } = { from, to, limit: 200, ...(status ? { status } : {}) };
    if (cursor) query.cursor = cursor;
    const page: { items: Shift[]; next_cursor?: string | null } = await call(api.GET("/api/v1/shifts", { params: { query } }));
    all.push(...page.items);
    cursor = page.next_cursor ?? null;
  } while (cursor);
  return all;
}
