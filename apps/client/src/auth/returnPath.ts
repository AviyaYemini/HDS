/**
 * Where sign-in sends a person afterwards (specs/007-ux-redesign contracts/ui-routes.md, "Deep
 * links"): back to the address of this app they were sent from, or home.
 *
 * The value arrives in the address (`/sign-in?next=…`), so it is treated as hostile until proven
 * harmless: decoded once, accepted only as a relative path of this app, refused for anything that
 * could leave the app (a second leading slash, a backslash, a scheme) or smuggle a header (a
 * control character), and checked against the app's own routes. A refused value goes home, quietly.
 */

/** The first path segments of every signed-in address this app has. */
export const KNOWN_PREFIXES: readonly string[] = [
  "/home",
  "/my",
  "/roster",
  "/runs",
  "/reports",
  "/admin",
  "/manage",
  "/more",
  "/client",
  "/account",
  "/inbox",
  "/no-access",
  "/help",
  "/accessibility",
  "/contact",
  "/dev/gallery",
];

const CONTROL = /[\u0000-\u001f\u007f]/;

/** A return path this app accepts, or null. */
export function safeReturnPath(raw: string | string[] | null | undefined): string | null {
  const text = Array.isArray(raw) ? raw[0] : raw;
  if (typeof text !== "string" || text === "") return null;
  let decoded: string;
  try {
    decoded = decodeURIComponent(text);
  } catch {
    return null;
  }
  if (decoded.length > 2048) return null;
  if (CONTROL.test(decoded) || decoded.includes("\\")) return null;
  if (!decoded.startsWith("/")) return null;
  const second = decoded.charAt(1);
  if (second === "" || second === "/" || second === "\\") return null;
  const queryAt = decoded.indexOf("?");
  const path = queryAt === -1 ? decoded : decoded.slice(0, queryAt);
  if (path.includes(":")) return null;
  if (!KNOWN_PREFIXES.some((prefix) => path === prefix || path.startsWith(`${prefix}/`))) return null;
  return decoded;
}

/** The `next` value for a sign-in redirect: `pathname` with its `query`, encoded once. */
export function encodeReturnPath(pathname: string, query: Record<string, string | string[] | undefined>): string {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value === undefined) continue;
    for (const item of Array.isArray(value) ? value : [value]) params.append(key, item);
  }
  const search = params.toString();
  return encodeURIComponent(search ? `${pathname}?${search}` : pathname);
}
