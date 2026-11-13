// Record the demo walkthrough as frames with Hebrew captions (CEO demo).
//
//   node scripts/demo/record_walkthrough.mjs OUT_DIR [--mode=wide|tall]
//   then: compose_video OUT_DIR OUT.mp4 [1080 1920]   (scripts/demo/compose_video.swift)
//
// wide: the desktop app, about a minute, for a laptop or a projector (1920x1080).
// tall: the same tour on a phone-sized screen, for a vertical video (1080x1920).
//
// Drives the running demo (scripts/demo/run_demo.sh: web on :8081, API on :8000) in Hebrew with the
// client's Playwright, taking screenshots as fast as it can, and writes manifest.json with each
// frame's time and the caption shown with it; the compositor resamples to a steady 30 fps. Waiting
// for a screen to load is not filmed, so cuts are instant. No voice: the captions explain. The
// frames show the demo's data, which may hold real names: keep OUT_DIR in the git-ignored demo-data.
import { mkdirSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const require = createRequire(resolve(here, "../../apps/client/package.json"));
const { chromium } = require("@playwright/test");

const WEB = process.env.DEMO_WEB ?? "http://localhost:8081";
const PASSWORD = process.env.DEMO_PASSWORD ?? "Dev1234!";
const out = resolve(process.argv[2] ?? "demo-data/video");
const mode = (process.argv.find((a) => a.startsWith("--mode=")) ?? "--mode=wide").slice(7);
mkdirSync(out, { recursive: true });

const frames = [];
let clock = 0;
let last = Date.now();
let caption = { eyebrow: "", caption: "" };

function tick() {
  const now = Date.now();
  clock += (now - last) / 1000;
  last = now;
  return clock;
}

async function snap(page) {
  const file = `f${String(frames.length).padStart(5, "0")}.jpg`;
  await page.screenshot({ path: join(out, file), type: "jpeg", quality: 90 });
  frames.push({ file, t: tick(), ...caption });
}

async function hold(page, seconds) {
  const end = Date.now() + seconds * 1000;
  while (Date.now() < end) await snap(page);
}

function say(eyebrow, text) {
  caption = { eyebrow, caption: text };
}

function title(lines, sub, seconds, eyebrow = "HYBRID DEFENSE SYSTEMS") {
  tick();
  frames.push({ t: clock, title: lines, sub, eyebrow });
  clock += seconds;
  last = Date.now();
}

/** Scroll smoothly: many small wheel steps, filming each. */
async function scroll(page, pixels, steps = 24) {
  const box = page.viewportSize();
  await page.mouse.move(box.width * 0.45, box.height * 0.6);
  for (let i = 0; i < steps; i++) {
    await page.mouse.wheel(0, pixels / steps);
    await snap(page);
  }
}

/** Wait for a screen to load; the wait is not filmed, so the cut to it is instant. */
async function settle(page, ms = 700) {
  await page.waitForLoadState("networkidle").catch(() => {});
  await page.waitForTimeout(ms);
  last = Date.now();
}

async function signIn(page, email, landing) {
  await page.goto(`${WEB}/sign-in?lang=he`);
  await page.getByLabel("דוא״ל").waitFor({ timeout: 60000 });
  await settle(page, 400);
  return async (film = true) => {
    await page.getByLabel("דוא״ל").fill(email);
    await page.getByLabel("סיסמה").fill(PASSWORD);
    if (film) await hold(page, 0.6);
    await page.getByLabel("סיסמה").press("Enter");
    await page.waitForURL(landing, { timeout: 30000 });
    await settle(page, 500);
  };
}

async function newRun(page, film = true) {
  await page.goto(`${WEB}/runs`);
  await settle(page);
  await page.getByText("הרצה חדשה", { exact: true }).first().click();
  await settle(page, 400);
  if (film) await hold(page, 1.2);
  await page.getByText("כל פרויקטי האבטחה", { exact: true }).first().click();
  if (film) await hold(page, 0.8);
  await page.getByText("הפקה", { exact: true }).first().click();
  await page.waitForURL(/\/runs\/[0-9a-f-]{36}/, { timeout: 30000 });
  await settle(page, 200);
}

async function wide(browser) {
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, locale: "he-IL", colorScheme: "light" });
  const page = await ctx.newPage();
  say("כניסה", "מסך הכניסה בשפת המותג: מנהלים, עובדים ולקוחות נכנסים מאותו מקום.");
  const go = await signIn(page, "admin@hds.test", /manage\/overview/);
  await hold(page, 1.6);
  await go();

  say("01 · מבט כולל", "תמונת המצב של החודש — חיוב, תחזית, כיסוי ועובדים — ישר מהסידור.");
  await hold(page, 3.6);
  say("02 · גרפים", "חיוב מול שכר לפי שבוע, נרות של חיוב יומי, חיוב לפי לקוח והרכב כוח האדם.");
  await scroll(page, 420);
  await hold(page, 2.4);
  await page.getByText("רבעון", { exact: true }).first().click().catch(() => {});
  await hold(page, 2.6);
  await page.getByText("החודש", { exact: true }).first().click().catch(() => {});
  await settle(page, 300);
  say("03 · פרויקטים", "לכל פרויקט: שעות, חיוב, כיסוי, מרווח וסטטוס, עם מגמת השעות.");
  await scroll(page, 700);
  await hold(page, 2.6);

  say("04 · סידור עבודה", "כמו הגיליון: פרויקטים לפי לקוח, ימים בעמודות, ומנהל המשימה לכל פרויקט.");
  await page.goto(`${WEB}/roster`);
  await settle(page);
  await hold(page, 3);
  const block = page.locator('[data-testid^="sheet-person"]').nth(2);
  if (await block.count()) {
    say("04 · סידור עבודה", "מעבר על שם: שעות העובד בסידור השבוע.");
    await block.hover().catch(() => {});
    await hold(page, 2.2);
    await page.mouse.move(5, 5);
  }
  say("05 · השבוע הבא", "השבוע הבא בטיוטה: מקומות פנויים מסומנים בקו מקווקו.");
  await page.getByText("שבוע הבא").first().click().catch(() => {});
  await settle(page);
  await hold(page, 2.6);

  say("06 · שיבוץ אוטומטי", "הרצת שיבוץ לשבוע הבא, לכל פרויקטי האבטחה.");
  await newRun(page);
  say("06 · שיבוץ אוטומטי", "המערכת מחפשת את הסידור הטוב ביותר…");
  await hold(page, 1.4);
  await page.getByText("אופטימלי").first().waitFor({ timeout: 30000 }).catch(() => {});
  await settle(page, 400);
  say("06 · שיבוץ אוטומטי", "תוך שניות — כל המקומות מאוישים. המנהל מאשר הכול, לפי פרויקט או לפי יום.");
  await hold(page, 2.8);
  await scroll(page, 520);
  await hold(page, 1.6);

  say("07 · צוות", "שעות השבוע והחודש לכל עובד. כל עובד קבוע, אלא אם סומן כזמני.");
  await page.goto(`${WEB}/admin/staff`);
  await settle(page);
  await hold(page, 2.6);
  const invite = page.getByText("הזמנת עובד", { exact: true }).first();
  if (await invite.count()) {
    await invite.click().catch(() => {});
    await settle(page, 400);
    say("07 · צוות", "עובד זמני: עד שלושה חודשים, על פרויקטים מסוימים — נקבע בהזמנה או בכרטיס.");
    await page.getByText("זמני", { exact: true }).last().click().catch(() => {});
    await hold(page, 3);
    await page.keyboard.press("Escape").catch(() => {});
  }

  say("08 · שעות ועלויות", "שעות, חיוב ושכר לפי פרויקט, עם המרווח — מה שלקח יום בגיליון.");
  await page.goto(`${WEB}/reports/hours`);
  await settle(page);
  await hold(page, 3.2);
  await ctx.close();
}

async function tall(browser) {
  const phone = {
    viewport: { width: 390, height: 844 }, deviceScaleFactor: 3, isMobile: true, hasTouch: true,
    locale: "he-IL", colorScheme: "light",
  };
  const ctx = await browser.newContext(phone);
  const page = await ctx.newPage();
  say("כניסה", "HDS בטלפון: אותה מערכת, מותאמת למסך קטן.");
  const go = await signIn(page, "admin@hds.test", /manage\/overview/);
  await hold(page, 1.6);
  await go();

  say("01 · מבט כולל", "תמונת המצב של החודש: חיוב, תחזית, כיסוי ועובדים.");
  await hold(page, 3.2);
  say("02 · גרפים", "חיוב מול שכר, נרות של חיוב יומי, ולקוחות.");
  await scroll(page, 1500, 36);
  await hold(page, 1.6);
  say("03 · פרויקטים", "כל פרויקט ככרטיס: חיוב, כיסוי ומרווח.");
  await scroll(page, 1300, 30);
  await hold(page, 1.8);

  say("04 · סידור עבודה", "הסידור יום אחר יום, עם מנהל המשימה לכל פרויקט.");
  await page.goto(`${WEB}/roster`);
  await settle(page);
  await hold(page, 2.6);
  await scroll(page, 700, 20);
  await hold(page, 1.2);

  say("05 · שיבוץ אוטומטי", "הרצת שיבוץ לשבוע הבא — מהטלפון.");
  await newRun(page, false);
  await hold(page, 1.2);
  await page.getByText("אופטימלי").first().waitFor({ timeout: 30000 }).catch(() => {});
  await settle(page, 400);
  say("05 · שיבוץ אוטומטי", "כל המקומות מאוישים תוך שניות; האישור נמצא תמיד בתחתית המסך.");
  await hold(page, 2.6);
  await scroll(page, 900, 24);
  await hold(page, 1.4);

  say("06 · צוות", "העובדים ככרטיסים, מהעמוס ביותר.");
  await page.goto(`${WEB}/admin/staff`);
  await settle(page);
  await hold(page, 2.6);
  await ctx.close();

  const ctx2 = await browser.newContext(phone);
  const emp = await ctx2.newPage();
  const goEmp = await signIn(emp, "emp001@demo.hds.test", /home/);
  await goEmp(false);
  say("07 · האפליקציה של העובד", "המשמרת הבאה, מועד הגשת הזמינות והמשמרות.");
  await hold(emp, 3.2);
  say("07 · האפליקציה של העובד", "והשכר הצפוי לחודש, לפי תעריפים.");
  await emp.goto(`${WEB}/my/pay`);
  await settle(emp);
  await hold(emp, 3);
  await ctx2.close();
}

const browser = await chromium.launch();
title(["HDS 2.0"], mode === "tall" ? "סיור קצר באפליקציה" : "סיור קצר במערכת", 2.2);
await (mode === "tall" ? tall : wide)(browser);
title(["היתרון האנושי.", "הפתרון הטכנולוגי."], "HDS 2.0 · סידור, צוות ותמונת מצב", 2.6);
await browser.close();
writeFileSync(join(out, "manifest.json"), JSON.stringify({ frames, end: clock }, null, 1));
console.log(`record (${mode}): ${frames.length} frames, ${clock.toFixed(1)} s → ${out}`);
