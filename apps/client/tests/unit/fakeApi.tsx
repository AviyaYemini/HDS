// A fake HDS API for the tests: answers by method and path, and records every request it gets.
import { render } from "@testing-library/react-native";
import type { ReactElement } from "react";

import { Session } from "../../src/api/session";
import { SessionProvider } from "../../src/api/SessionContext";
import { memoryStore, type TokenStore } from "../../src/api/tokens";

export const BASE = "http://api.test";

export interface Seen {
  method: string;
  path: string;
  query: string;
  auth: string | null;
  body: unknown;
}

type Answer = Response | ((seen: Seen) => Response | Promise<Response>);

/** A JSON answer. */
export function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": status >= 400 ? "application/problem+json" : "application/json" },
  });
}

/** A problem answer with ``code``. */
export function problem(status: number, code: string): Response {
  return json(status, { type: "about:blank", title: "x", status, code });
}

export class FakeApi {
  readonly seen: Seen[] = [];
  private readonly queues = new Map<string, Answer[]>();

  /** Answer ``method path`` with ``answers`` in turn; the last one repeats. */
  on(method: string, path: string, ...answers: Answer[]): this {
    this.queues.set(`${method} ${path}`, answers);
    return this;
  }

  /** Whether ``method path`` has answers set. */
  answers(method: string, path: string): boolean {
    return this.queues.has(`${method} ${path}`);
  }

  /** The requests made to ``method path``. */
  calls(method: string, path: string): Seen[] {
    return this.seen.filter((s) => s.method === method && s.path === path);
  }

  readonly fetch = async (request: Request): Promise<Response> => {
    const url = new URL(request.url);
    const text = await request.text();
    const seen: Seen = {
      method: request.method,
      path: url.pathname,
      query: url.search,
      auth: request.headers.get("Authorization"),
      body: text ? JSON.parse(text) : undefined,
    };
    this.seen.push(seen);
    const queue = this.queues.get(`${seen.method} ${seen.path}`);
    if (!queue || queue.length === 0) return problem(404, "resource.not_found");
    const next = queue.length > 1 ? queue.shift()! : queue[0]!;
    const answer = typeof next === "function" ? await next(seen) : next;
    return answer.clone();
  };
}

export const ME = {
  id: "11111111-1111-4111-8111-111111111111",
  organisation_id: "22222222-2222-4222-8222-222222222222",
  email: "employee@hds.test",
  full_name: "Dana Employee",
  role: "employee",
  is_active: true,
  created_at: "2027-01-01T00:00:00Z",
  updated_at: "2027-01-01T00:00:00Z",
};

export const TOKENS = { access_token: "a1", refresh_token: "r1", expires_in: 900, token_type: "bearer" };

/** A session over ``api``, signed in as the employee ``ME``. */
export async function signedIn(api: FakeApi, store: TokenStore = memoryStore()): Promise<Session> {
  api.on("POST", "/auth/login", json(200, TOKENS));
  if (!api.answers("GET", "/me")) api.on("GET", "/me", json(200, ME));
  const session = new Session(BASE, store, api.fetch);
  expect(await session.signIn("employee@hds.test", "Dev1234!")).toBeNull();
  return session;
}

/** Render ``ui`` inside ``session``. */
export function renderWith(session: Session, ui: ReactElement) {
  return render(<SessionProvider session={session}>{ui}</SessionProvider>);
}
