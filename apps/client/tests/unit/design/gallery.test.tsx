/**
 * The development gallery (specs/007-ux-redesign FR-067, task T020; review finding R11): it draws
 * every section from the strings file, its controls act (theme, segments, sorting, a toast, a
 * preset, the date field), and the loading button runs through the loading store.
 */
import { act, fireEvent, render, screen } from "@testing-library/react-native";

import { ToastHost } from "../../../src/design/components";
import { getLoadingState, resetLoading } from "../../../src/design/loading/loadingStore";
import { ThemeProvider } from "../../../src/design/theme/ThemeProvider";
import { resetThemeStore } from "../../../src/design/theme/themeStore";
import { DEFAULT_TIME_ZONE, formatNumericRange, presetRange, stripIsolates, todayIn } from "../../../src/locale";
import { GalleryScreen } from "../../../src/screens/gallery/GalleryScreen";

beforeEach(() => {
  resetLoading();
  resetThemeStore();
});

async function open() {
  return render(
    <ThemeProvider>
      <GalleryScreen />
      <ToastHost />
    </ThemeProvider>,
  );
}

describe("GalleryScreen (FR-067)", () => {
  it("draws every section with translated titles", async () => {
    await open();
    for (const title of ["Buttons", "Fields", "Status chips", "Feature panel", "Direction", "Table", "Dates", "Feedback", "Surfaces", "Loading"]) {
      expect(screen.getByText(title)).toBeTruthy();
    }
    expect(screen.getByTestId("gallery-title")).toHaveTextContent("Building blocks");
    expect(screen.getByTestId("gallery-cta")).toBeTruthy();
    expect(screen.getByTestId("gallery-time")).toHaveTextContent(/06:00–14:00/);
  });

  it("switches the theme, the segment and the sort, and shows a toast", async () => {
    await open();
    await fireEvent.press(screen.getByRole("radio", { name: "Dark" }));
    expect(screen.getByRole("radio", { name: "Dark", checked: true })).toBeTruthy();
    await fireEvent.press(screen.getByTestId("gallery-segment-pay"));
    expect(screen.getByRole("radio", { name: "Pay", checked: true })).toBeTruthy();
    await fireEvent.press(screen.getByRole("button", { name: "Hours" }));
    await fireEvent.press(screen.getByRole("button", { name: "Hours" }));
    await fireEvent.press(screen.getByTestId("gallery-toast"));
    expect(screen.getByText("Availability submitted")).toBeTruthy();
    // The banner has a Dismiss too; the toast's is the last one rendered.
    const dismiss = screen.getAllByRole("button", { name: "Dismiss" });
    await fireEvent.press(dismiss[dismiss.length - 1]!);
  });

  it("drives the pickers and opens the sheet", async () => {
    await open();
    await fireEvent.press(screen.getByTestId("preset-last-week"));
    // The gallery reads the real clock, so the expected week is computed the same way (R18).
    const lastWeek = presetRange("last-week", todayIn(DEFAULT_TIME_ZONE));
    expect(stripIsolates(screen.getByTestId("date-range-summary").props.children)).toBe(
      stripIsolates(formatNumericRange(lastWeek.start, lastWeek.end)),
    );
    const field = screen.getByTestId("date-field");
    await fireEvent.changeText(field, "7/11/2026");
    await fireEvent(field, "blur");
    expect(field.props.value).toBe("07/11/2026");
    await fireEvent.press(screen.getByTestId("gallery-open-sheet"));
    expect(screen.getByTestId("gallery-sheet")).toBeTruthy();
    await fireEvent.press(screen.getByTestId("gallery-sheet-close"));
    await fireEvent.press(screen.getByTestId("gallery-picker"));
    await fireEvent.press(screen.getByTestId("gallery-picker-option-p3"));
    expect(screen.getByTestId("gallery-picker")).toHaveTextContent(/Yosef Cohen/);
  });

  it("runs a load through the loading store", async () => {
    const fetchMock = jest.fn(async () => new Response("{}", { status: 200 }));
    const original = globalThis.fetch;
    globalThis.fetch = fetchMock as unknown as typeof fetch;
    try {
      await open();
      await act(async () => {
        await fireEvent.press(screen.getByTestId("gallery-load"));
      });
      expect(fetchMock).toHaveBeenCalledWith(expect.stringMatching(/\/health$/));
      expect(await screen.findByTestId("gallery-load-count")).toHaveTextContent(/1/);
      expect(getLoadingState().visible).toBe(false);
    } finally {
      globalThis.fetch = original;
    }
  });
});
