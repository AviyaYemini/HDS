// Serve the exported web build (`dist/`) for the e2e specs: files as they are, and index.html for
// any other path, so the app's router handles deep links such as /sign-in.
import { createReadStream, existsSync, statSync } from "node:fs";
import { createServer } from "node:http";
import { extname, join, normalize, sep } from "node:path";

const ROOT = join(import.meta.dirname, "../../dist");
const PORT = Number(process.env.PORT ?? 3000);
const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json",
  ".png": "image/png",
  ".ico": "image/x-icon",
  ".svg": "image/svg+xml",
  ".ttf": "font/ttf",
};

createServer((request, response) => {
  const path = decodeURIComponent(new URL(request.url ?? "/", "http://localhost").pathname);
  const asked = normalize(join(ROOT, path));
  const inside = asked === ROOT || asked.startsWith(ROOT + sep);
  const file = inside && existsSync(asked) && statSync(asked).isFile() ? asked : join(ROOT, "index.html");
  response.writeHead(200, { "Content-Type": TYPES[extname(file)] ?? "application/octet-stream" });
  createReadStream(file).pipe(response);
}).listen(PORT, "localhost", () => console.log(`serving ${ROOT} on http://localhost:${PORT}`));
