/**
 * The top bar's language and light/dark controls (the owner, PR 4): each switches to the other,
 * by the same path as Account, and the theme choice is kept.
 */
import { fireEvent, render, screen } from "@testing-library/react-native";

import { ThemeProvider } from "../../src/design/theme/ThemeProvider";
import { getThemeState, resetThemeStore } from "../../src/design/theme/themeStore";
import { i18n, setupI18n } from "../../src/i18n";
import { LanguageToggle, ThemeToggle, otherLanguage, toggledTheme } from "../../src/navigation/DisplayControls";

beforeEach(async () => {
  resetThemeStore();
  await setupI18n("en");
});

test("the theme toggles light and dark, keeping high contrast", () => {
  expect(toggledTheme("light")).toBe("dark");
  expect(toggledTheme("dark")).toBe("light");
  expect(toggledTheme("highContrastLight")).toBe("highContrastDark");
  expect(toggledTheme("highContrastDark")).toBe("highContrastLight");
});

test("the language switch offers the other language", () => {
  expect(otherLanguage("he")).toBe("en");
  expect(otherLanguage("en")).toBe("he");
  expect(otherLanguage("en-GB")).toBe("he");
});

test("pressing the theme toggle stores the opposite of what shows, from following the device", async () => {
  await render(
    <ThemeProvider>
      <ThemeToggle />
    </ThemeProvider>,
  );
  expect(getThemeState().preference).toBe("system");
  // The device is light in tests, so the toggle offers dark mode.
  expect(screen.getByLabelText("Dark mode")).toBeTruthy();
  fireEvent.press(screen.getByTestId("action-toggle-theme"));
  expect(getThemeState().preference).toBe("dark");
  expect(await screen.findByLabelText("Light mode")).toBeTruthy();
});

test("pressing the language switch changes the language, by its own name", async () => {
  await render(
    <ThemeProvider>
      <LanguageToggle />
    </ThemeProvider>,
  );
  const button = screen.getByTestId("action-switch-language");
  expect(screen.getByLabelText("Language: עברית")).toBeTruthy();
  fireEvent.press(button);
  await screen.findByLabelText("שפה: English");
  expect(i18n.language).toBe("he");
});
