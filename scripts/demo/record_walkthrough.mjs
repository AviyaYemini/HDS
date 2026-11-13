// Record the demo walkthrough as frames with Hebrew captions (CEO demo).
//
//   node scripts/demo/record_walkthrough.mjs OUT_DIR [--only=scene,scene]
//   then: compose_video OUT_DIR OUT.mp4   (scripts/demo/compose_video.swift)
//
// Drives the running demo (scripts/demo/run_demo.sh: web on :8081, API on :8000) in Hebrew with the
// client's Playwright, taking a screenshot about ten times a second, and writes manifest.json with
// each frame's time and the caption shown with it. No voice: the captions explain what we see. The
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
const only = (process.argv.find((a) => a.startsWith("--only=")) ?? "").slice(7).split(",").filter(Boolean);
mkdirSync(out, { recursive: true });

const frames = [];
let clock = 0; // seconds of video so far
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
  await page.screenshot({ path: join(out, file), type: "jpeg", quality: 88 });
  frames.push({ file, t: tick(), ...caption });
}

/** Keep filming for ``seconds`` of real time (animations play, captions stay up). */
async function hold(page, seconds) {
  const end = Date.now() + seconds * 1000;
  while (Date.now() < end) await snap(page);
}

function say(eyebrow, text) {
  caption = { eyebrow, caption: text };
}

/** A title card for ``seconds`` of video, drawn by the compositor; no real time passes. */
function title(lines, sub, seconds, eyebrow = "HYBRID DEFENSE SYSTEMS") {
  tick();
  frames.push({ t: clock, title: lines, sub, eyebrow });
  clock += seconds;
  last = Date.now();
}

/** Scroll the screen's own scroller by ``pixels`` in small steps, filming as it goes. */
async function scroll(page, pixels, steps = 12) {
  const box = page.viewportSize();
  await page.mouse.move(box.width * 0.45, box.height * 0.6);
  for (let i = 0; i < steps; i++) {
    await page.mouse.wheel(0, pixels / steps);
    await snap(page);
  }
}

async function signIn(page, email) {
  await page.goto(`${WEB}/sign-in?lang=he`);
  await page.getByLabel("דוא״ל").waitFor();
  return async (film = true) => {
    await page.getByLabel("דוא״ל").fill("");
    for (const ch of email) {
      await page.getByLabel("דוא״ל").press(ch === "@" ? "@" : ch);
      if (film) await snap(page);
    }
    await page.getByLabel("סיסמה").fill(PASSWORD);
    if (film) await hold(page, 0.4);
    await page.getByLabel("סיסמה").press("Enter");
  };
}

async function settle(page, ms = 900) {
  await page.waitForLoadState("networkidle").catch(() => {});
  await page.waitForTimeout(ms);
}

const scenes = {
  async desktop(browser) {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, locale: "he-IL", colorScheme: "light" });
    const page = await ctx.newPage();

    say("01 · כניסה", "מסך הכניסה בשפת המותג. מנהלים, עובדים ולקוחות נכנסים מאותו מקום.");
    const type = await signIn(page, "admin@hds.test");
    await hold(page, 2.5);
    await type();
    await page.waitForURL(/manage\/overview/, { timeout: 20000 });

    say("02 · מבט כולל", "תמונת המצב של החודש: חיוב בפועל ותחזית, כיסוי המשמרות והעובדים — מתוך הסידור עצמו.");
    await hold(page, 6);
    say("02 · מבט כולל", "כל נתון מושווה לחודש הקודם עד אותו יום, כדי שההשוואה תהיה הוגנת.");
    await hold(page, 4.5);
    say("03 · גרפים", "חיוב מול שכר לפי שבוע, נרות של חיוב יומי, חיוב לפי לקוח והרכב כוח האדם.");
    await scroll(page, 420);
    await hold(page, 5);
    await page.getByText("רבעון", { exact: true }).first().click().catch(() => {});
    say("03 · גרפים", "ברבעון רואים את המגמה: כל נר הוא שבוע — פתיחה, סגירה, השיא והשפל.");
    await hold(page, 5);
    await page.getByText("החודש", { exact: true }).first().click().catch(() => {});
    await settle(page, 400);
    say("04 · פרויקטים", "לכל פרויקט: שעות, חיוב, כיסוי, מרווח וסטטוס, עם קו מגמה של השעות.");
    await scroll(page, 700);
    await hold(page, 5.5);

    say("05 · סידור עבודה", "הסידור בדיוק כמו הגיליון: פרויקטים לפי לקוח, ימים בעמודות, ומנהל המשימה לכל פרויקט.");
    await page.goto(`${WEB}/roster`);
    await settle(page);
    await hold(page, 6);
    const block = page.locator('[data-testid^="sheet-person"]').nth(2);
    if (await block.count()) {
      say("05 · סידור עבודה", "מעבר על שם מציג כרטיס: שעות העובד בסידור השבוע, ואם הוא זמני — עד מתי.");
      await block.hover().catch(() => {});
      await hold(page, 4);
      await page.mouse.move(5, 5);
    }
    say("06 · השבוע הבא", "השבוע הבא עדיין בטיוטה: מקומות פנויים מסומנים בקו מקווקו, לפני הפרסום.");
    await page.getByText("שבוע הבא").first().click().catch(() => {});
    await settle(page);
    await hold(page, 5.5);

    say("07 · שיבוץ אוטומטי", "הרצת שיבוץ: בוחרים את השבוע הבא ואת כל פרויקטי האבטחה, ולוחצים הפקה.");
    await page.goto(`${WEB}/runs`);
    await settle(page);
    await page.getByText("הרצה חדשה", { exact: true }).first().click();
    await settle(page, 600);
    await hold(page, 2.5);
    await page.getByText("כל פרויקטי האבטחה", { exact: true }).first().click();
    await hold(page, 2);
    await page.getByText("הפקה", { exact: true }).first().click();
    await page.waitForURL(/\/runs\/[0-9a-f-]{36}/, { timeout: 30000 });
    say("07 · שיבוץ אוטומטי", "תוך שניות כל המקומות מאוישים. עובדים זמניים נשארים בפרויקטים ובתאריכים שלהם.");
    await settle(page, 1500);
    await hold(page, 6);
    say("07 · שיבוץ אוטומטי", "ההצעה מוצגת כשבוע סידור, עם השינויים מסומנים. המנהל מאשר הכול, לפי פרויקט או לפי יום.");
    await scroll(page, 520);
    await hold(page, 5.5);

    say("08 · צוות", "שעות השבוע והחודש לכל עובד, מהעמוס ביותר; סינון קבועים וזמניים.");
    await page.goto(`${WEB}/admin/staff`);
    await settle(page);
    await hold(page, 5);
    say("08 · צוות", "כל עובד הוא קבוע, אלא אם מנהל סימן אותו כזמני: בהזמנה, ביצירה או בכרטיס העובד.");
    const invite = page.getByText("הזמנת עובד", { exact: true }).first();
    if (await invite.count()) {
      await invite.click().catch(() => {});
      await settle(page, 600);
      await hold(page, 2.5);
      const temp = page.getByText("זמני", { exact: true }).last();
      say("08 · צוות", "עובד זמני: עד שלושה חודשים, על פרויקטים מסוימים. השיבוץ האוטומטי לא יציב אותו מחוץ להם.");
      await temp.click().catch(() => {});
      await hold(page, 5);
      await page.keyboard.press("Escape").catch(() => {});
      await settle(page, 400);
    }

    say("09 · שעות ועלויות", "שעות, חיוב ושכר ברוטו לפי פרויקט, עם המרווח — מה שהיה לוקח יום בגיליון.");
    await page.goto(`${WEB}/reports/hours`);
    await settle(page);
    await hold(page, 6);

    say("10 · מעקב הגשות", "חלון הזמינות לשבוע שאחרי: מי הגיש, כמה נשאר, ומתי המועד האחרון.");
    await page.goto(`${WEB}/roster/submissions`);
    await settle(page);
    await hold(page, 4.5);
    await ctx.close();
  },

  async phone(browser) {
    const ctx = await browser.newContext({
      viewport: { width: 390, height: 844 }, deviceScaleFactor: 2, isMobile: true, hasTouch: true,
      locale: "he-IL", colorScheme: "light",
    });
    const page = await ctx.newPage();
    const type = await signIn(page, "admin@hds.test");
    await type(false);
    await page.waitForURL(/manage\/overview/, { timeout: 20000 });
    await settle(page);
    say("11 · בטלפון", "אותה מערכת בטלפון: מספרים וגרפים בעמודה אחת, וכרטיסים במקום טבלאות.");
    await hold(page, 5);
    await scroll(page, 1400, 18);
    await hold(page, 2.5);
    say("11 · בטלפון", "הסידור בטלפון: יום אחר יום, עם כל הפרויקטים של היום כרטיסים.");
    await page.goto(`${WEB}/roster`);
    await settle(page);
    await hold(page, 5);
    await ctx.close();

    const ctx2 = await browser.newContext({
      viewport: { width: 390, height: 844 }, deviceScaleFactor: 2, isMobile: true, hasTouch: true,
      locale: "he-IL", colorScheme: "light",
    });
    const emp = await ctx2.newPage();
    const typeEmp = await signIn(emp, "emp001@demo.hds.test");
    await typeEmp(false);
    await emp.waitForURL(/home/, { timeout: 20000 });
    await settle(emp);
    say("12 · האפליקציה של העובד", "כל עובד רואה את המשמרת הבאה, את מועד הגשת הזמינות ואת המשמרות שלו.");
    await hold(emp, 5.5);
    say("12 · האפליקציה של העובד", "ואת השכר הצפוי לחודש, לפי תעריפים, מושווה לאותם ימים בחודש הקודם.");
    await emp.goto(`${WEB}/my/pay`);
    await settle(emp);
    await hold(emp, 5.5);
    await ctx2.close();
  },
};

const browser = await chromium.launch();
title(["HDS 2.0", "סיור במערכת"], "על נתוני הסידור האמיתיים · נובמבר 2026", 4);
for (const name of ["desktop", "phone"]) {
  if (only.length && !only.includes(name)) continue;
  last = Date.now();
  await scenes[name](browser);
}
title(["היתרון האנושי.", "הפתרון הטכנולוגי."], "HDS 2.0 · סידור, צוות ותמונת מצב — במקום אחד", 4);
await browser.close();
writeFileSync(join(out, "manifest.json"), JSON.stringify({ frames, end: clock }, null, 1));
console.log(`record: ${frames.length} frames, ${clock.toFixed(1)} s → ${out}`);
