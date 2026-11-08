/**
 * Group B — role shells and navigation (specs/007-ux-redesign, task T025).
 *
 * Covers FR-001 to FR-009, FR-012, FR-059, FR-068, FR-069 and FR-077 (the 404, offline and splash
 * parts), in Hebrew and English, at desktop and phone sizes. The test ids and tab sets are in
 * `specs/007-ux-redesign/contracts/ui-routes.md` ("Navigation test ids", "Preview stubs"). Tests
 * tagged `@previews-off` run only against a build made without previews (`PREVIEWS=0`); the rest
 * need previews on.
 */
import { expect, test, type Page } from "@playwright/test";

import {
  API_URL,
  LANGUAGES,
  PREVIEWS_ON,
  type Role,
  apiHeaders,
  at,
  checkA11y,
  isPhone,
  openAs,
  signIn,
} from "./support";

const PHONE_TABS: Record<Role, string[]> = {
  employee: ["home", "availability", "my-shifts", "pay", "more"],
  manager: ["home", "roster", "my-work", "reports", "more"],
  admin: ["home", "roster", "my-work", "reports", "more"],
  client: ["projects", "contact"],
};

const EMPLOYEE_NAV = ["home", "availability", "my-shifts", "pay"];
const MANAGER_NAV = [
  ...EMPLOYEE_NAV,
  "roster",
  "submissions",
  "runs",
  "work-log",
  "hours",
  "staff",
  "clients",
  "sites",
  "projects",
  "shift-templates",
  "staffing-needs",
  "pay-settings",
];
const DESKTOP_NAV: Record<Role, string[]> = {
  employee: EMPLOYEE_NAV,
  manager: MANAGER_NAV,
  admin: [...MANAGER_NAV, "special-days"],
  client: ["client-home", "contact"],
};

/** Addresses a role must never reach, with the role's own home to compare. */
const FORBIDDEN: Record<Role, string[]> = {
  employee: ["/roster", "/admin/staff", "/reports/work-log", "/client"],
  manager: ["/admin/special-days", "/client"],
  admin: ["/client"],
  client: ["/my/availability", "/my/shifts", "/my/pay", "/roster", "/admin/staff"],
};

const ROUTED_PREVIEWS: { path: string; id: string; roles: Role[] }[] = [
  { path: "/client/billing", id: "preview-billing", roles: ["client"] },
  { path: "/client/requests", id: "preview-requests", roles: ["client"] },
  { path: "/manage/requests", id: "preview-requests", roles: ["manager", "admin"] },
  { path: "/manage/overview", id: "preview-overview", roles: ["manager", "admin"] },
  { path: "/manage/approvals", id: "preview-approvals", roles: ["manager", "admin"] },
  { path: "/my/requests", id: "preview-absence-requests", roles: ["employee", "manager", "admin"] },
  { path: "/inbox", id: "preview-inbox", roles: ["employee", "manager", "admin", "client"] },
  { path: "/account/notifications", id: "preview-notification-prefs", roles: ["employee", "manager", "admin", "client"] },
  { path: "/admin/audit-log", id: "preview-audit-log", roles: ["admin"] },
];

const REFUSAL = /not allowed|אין הרשאה|לא מורשה/i;

async function navKeys(page: Page, phone: boolean): Promise<string[]> {
  const container = page.getByTestId(phone ? "nav-tabs" : "nav-side");
  await expect(container).toBeVisible();
  const prefix = phone ? "tab-" : "nav-";
  return container.evaluate(
    (root, p) =>
      Array.from(root.querySelectorAll("[data-testid]"))
        .filter((n) => n.getAttribute("data-preview") !== "true")
        .map((n) => n.getAttribute("data-testid") ?? "")
        .filter((id) => id.startsWith(p) && !id.startsWith("nav-group-"))
        .map((id) => id.slice(p.length)),
    prefix,
  );
}

async function expectEverywhereControls(page: Page): Promise<void> {
  for (const id of ["action-sign-out", "link-help", "link-contact", "link-accessibility"]) {
    await expect(page.getByTestId(id).first(), id).toBeVisible();
  }
  await expect(page.locator("body")).not.toContainText(REFUSAL);
  await expect(page.getByTestId("screen-no-access")).toHaveCount(0);
}

test.describe("with previews on", () => {
  test.skip(!PREVIEWS_ON, "needs a build with previews on");

  for (const language of LANGUAGES) {
    for (const role of ["employee", "manager", "admin", "client"] as const) {
      test(`${role} in ${language}: offered exactly the role's destinations, none refused`, async ({ page }, testInfo) => {
        const phone = isPhone(testInfo);
        await signIn(page, role, language);
        const keys = await navKeys(page, phone);
        expect(keys.sort()).toEqual([...(phone ? PHONE_TABS[role] : DESKTOP_NAV[role])].sort());
        await expectEverywhereControls(page);
        await checkA11y(page, `${role} home (${language})`);

        // Open every destination offered and meet no refusal on any of them.
        const container = page.getByTestId(phone ? "nav-tabs" : "nav-side");
        for (const key of keys) {
          await container.getByTestId(`${phone ? "tab" : "nav"}-${key}`).click();
          await expect(page.getByTestId("nav-tabs").or(page.getByTestId("nav-side")).first()).toBeVisible();
          await expectEverywhereControls(page);
        }
      });

      test(`${role} in ${language}: a forbidden address shows the no-access page with a way home`, async ({ page }) => {
        for (const path of FORBIDDEN[role]) {
          await openAs(page, role, language, path);
          await expect(page.getByTestId("screen-no-access"), path).toBeVisible();
          await expect(page.getByTestId("action-go-home")).toBeVisible();
        }
        await page.getByTestId("action-go-home").click();
        await expect(page.getByTestId("screen-no-access")).toHaveCount(0);
        await checkA11y(page, `${role} after no-access (${language})`);
      });
    }

    test(`client in ${language}: sees no employee field anywhere it can go`, async ({ page }) => {
      for (const path of ["/client", "/client/contact", "/account"]) {
        await openAs(page, "client", language, path);
        await expect(page.locator("body")).not.toContainText(/Employee User|Extra Employee|employee@hds\.test|extra@hds\.test/);
        await expect(page.locator("body")).not.toContainText(/hourly rate|תעריף שעתי/i);
      }
    });

    test(`client in ${language}: the contact card shows phone, e-mail and opening hours`, async ({ page, request }) => {
      const admin = await apiHeaders(request, "admin");
      const set = await request.patch(`${API_URL}/api/v1/organisation/contact`, {
        headers: admin,
        data: {
          phone: "03-5550100",
          email: "office@hds.test",
          opening_hours: [{ weekday: 0, opens: "08:00", closes: "17:00" }],
        },
      });
      expect(set.ok(), await set.text()).toBeTruthy();
      await openAs(page, "client", language, "/client/contact");
      const card = page.getByTestId("screen-contact");
      await expect(card).toContainText("03-5550100");
      await expect(card).toContainText("office@hds.test");
      await expect(card).toContainText("08:00");
      await checkA11y(page, `contact (${language})`);
    });

    test(`${language}: every routed preview sits in place for its roles only`, async ({ page }) => {
      for (const role of ["employee", "manager", "admin", "client"] as const) {
        for (const preview of ROUTED_PREVIEWS) {
          await openAs(page, role, language, preview.path);
          if (preview.roles.includes(role)) {
            await expect(page.getByTestId(preview.id), `${role} ${preview.path}`).toBeVisible();
          } else {
            await expect(page.getByTestId(preview.id), `${role} ${preview.path}`).toHaveCount(0);
            await expect(page.getByTestId("screen-no-access")).toBeVisible();
          }
        }
        // The tab keeps the session (ADR 019), so the next role signs in afresh.
        await page.getByTestId("action-sign-out").first().click();
        await expect(page).toHaveURL(/\/sign-in/);
      }
    });
  }

  test("the language switch flips the direction straight away", async ({ page }) => {
    await openAs(page, "employee", "en", "/account");
    await expect(page.locator("html")).toHaveAttribute("dir", "ltr");
    await page.getByTestId("language-he").click();
    await expect(page.locator("html")).toHaveAttribute("dir", "rtl");
    await expect(page.locator("html")).toHaveAttribute("lang", "he");
    await page.getByTestId("language-en").click();
    await expect(page.locator("html")).toHaveAttribute("dir", "ltr");
  });

  for (const language of LANGUAGES) {
    // An unknown address is no one's page, so it shows the 404 page signed in or not.
    test(`${language}: an unknown address shows the 404 page with a way home`, async ({ page }) => {
      await page.goto(at("/no-such-page", language));
      await expect(page.getByTestId("screen-not-found")).toBeVisible();
      await expect(page.locator("html")).toHaveAttribute("lang", language);
      await page.getByTestId("action-go-home").click();
      await expect(page.getByTestId("screen-not-found")).toHaveCount(0);
    });

    test(`${language}: losing the network shows the offline banner`, async ({ page, context }) => {
      await signIn(page, "employee", language);
      await context.setOffline(true);
      await expect(page.getByTestId("banner-offline")).toBeVisible();
      await expect(page.getByTestId("banner-offline")).not.toBeEmpty();
      await context.setOffline(false);
      await expect(page.getByTestId("banner-offline")).toHaveCount(0);
    });

    test(`${language}: the splash shows until the app has started`, async ({ page }) => {
      let release: () => void = () => undefined;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });
      await page.route("**/_expo/static/js/**", async (route) => {
        await held;
        await route.continue();
      });
      const loading = page.goto(at("/sign-in", language));
      await expect(page.getByTestId("splash")).toBeVisible();
      await expect(page.locator("html")).toHaveAttribute("lang", language);
      release();
      await loading;
      await expect(page.getByTestId("sign-in-btn")).toBeVisible();
      await expect(page.getByTestId("splash")).toBeHidden();
    });
  }
});

test.describe("with previews off @previews-off", () => {
  test.skip(PREVIEWS_ON, "needs a build made without previews");

  // PR 2 review: with previews off (production) no role is offered a preview, and every tab and
  // side item it is offered opens a real screen, never the 404 page.
  for (const role of ["employee", "manager", "admin", "client"] as const) {
    test(`${role}: offered only real screens`, async ({ page }, testInfo) => {
      const phone = isPhone(testInfo);
      await signIn(page, role, "en");
      const container = page.getByTestId(phone ? "nav-tabs" : "nav-side");
      await expect(container).toBeVisible();
      await expect(container.locator('[data-preview="true"]')).toHaveCount(0);
      const keys = await navKeys(page, phone);
      expect(keys.sort()).toEqual([...(phone ? PHONE_TABS[role] : DESKTOP_NAV[role])].sort());
      const previews = new Set(ROUTED_PREVIEWS.map((p) => p.path));
      for (const key of keys) {
        await container.getByTestId(`${phone ? "tab" : "nav"}-${key}`).click();
        await expect(page.getByTestId("screen-not-found"), key).toHaveCount(0);
        expect(previews.has(new URL(page.url()).pathname), key).toBe(false);
      }
    });
  }

  // Without previews the addresses are not registered at all, so they are unknown addresses: the
  // 404 page, signed in or not, for every role alike.
  test("no preview address resolves", async ({ page }) => {
    for (const preview of ROUTED_PREVIEWS) {
      await page.goto(at(preview.path, "en"));
      await expect(page.getByTestId(preview.id), preview.path).toHaveCount(0);
      await expect(page.getByTestId("screen-not-found"), preview.path).toBeVisible();
    }
    await page.goto(at("/dev/gallery", "en"));
    await expect(page.getByTestId("gallery-root")).toHaveCount(0);
  });
});

// The owner's decision of 2026-11-08 (T099, ADR 019): the web keeps the session per tab.
test.describe("a session per tab", () => {
  test("a reload and a deep link in the same tab stay signed in", async ({ page }) => {
    await signIn(page, "employee", "en");
    await page.reload();
    await expect(page.getByTestId("sign-in-btn")).toHaveCount(0);
    await expect(page.getByTestId("nav-tabs").or(page.getByTestId("nav-side")).first()).toBeVisible();
    await page.goto(at("/account", "en"));
    await expect(page.getByTestId("screen-account")).toBeVisible();
  });

  test("a duplicated tab signs in again and the original stays signed in", async ({ page, context }) => {
    await signIn(page, "employee", "en");
    // A duplicate gets a copy of the tab's storage, which holds nothing while the page is open.
    const copy = await page.evaluate(() => JSON.stringify(Object.entries(sessionStorage)));
    const duplicate = await context.newPage();
    await duplicate.addInitScript((entries) => {
      for (const [key, value] of JSON.parse(entries) as [string, string][]) sessionStorage.setItem(key, value);
    }, copy);
    await duplicate.goto(at("/home", "en"));
    await expect(duplicate.getByTestId("sign-in-btn")).toBeVisible();
    await duplicate.close();
    // No refresh token was presented twice, so nothing ended the original's session.
    await page.reload();
    await expect(page.getByTestId("sign-in-btn")).toHaveCount(0);
    await page.goto(at("/account", "en"));
    await expect(page.getByTestId("screen-account")).toBeVisible();
  });
});
