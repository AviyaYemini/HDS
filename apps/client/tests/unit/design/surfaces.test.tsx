/**
 * The surfaces and feedback primitives (specs/007-ux-redesign FR-047, FR-063, FR-071, FR-074, task
 * T023; review finding R11): the loading bar's role and timing, sheets and side panels opening and
 * closing, the searchable picker filtering and choosing, toasts arriving and leaving, the feature
 * panel's inverse text, the page title's heading, the theme provider's override and the screen
 * frame, all through React Native Testing Library and behaviour rather than snapshots.
 */
import { act, fireEvent, render, screen } from "@testing-library/react-native";
import { Text } from "react-native";

import {
  ClippedBox,
  EmptyState,
  FeaturePanel,
  LoadingBar,
  PageTitle,
  SearchablePicker,
  Sheet,
  SidePanel,
  Skeleton,
  SkeletonRows,
  ToastHost,
  Txt,
  clearToasts,
  dismissToast,
  showToast,
  useBackgroundColor,
} from "../../../src/design/components";
import { Screen } from "../../../src/design/components/Screen";
import { Icon } from "../../../src/design/icons/Icon";
import { resetLoading, setProgress, startLoading } from "../../../src/design/loading/loadingStore";
import { setReducedMotionForTests } from "../../../src/design/motion";
import { ThemeProvider, useTheme } from "../../../src/design/theme/ThemeProvider";
import { resetThemeStore } from "../../../src/design/theme/themeStore";

beforeEach(() => {
  resetLoading();
  resetThemeStore();
  clearToasts();
  setReducedMotionForTests(false);
});

describe("LoadingBar (FR-074)", () => {
  beforeEach(() => jest.useFakeTimers());
  afterEach(() => jest.useRealTimers());

  it("is absent until a load has run 300 ms, then a labelled progress bar", async () => {
    await render(<LoadingBar />);
    expect(screen.queryByTestId("loading-bar")).toBeNull();
    const done = startLoading();
    await act(async () => {
      jest.advanceTimersByTime(300);
    });
    const bar = screen.getByTestId("loading-bar");
    expect(bar.props.role).toBe("progressbar");
    expect(bar.props.accessibilityLabel).toBe("Loading…");
    done();
    await act(async () => {
      jest.advanceTimersByTime(400);
    });
    expect(screen.queryByTestId("loading-bar")).toBeNull();
  });

  it("shows a determinate value with its side text", async () => {
    await render(<LoadingBar />);
    const done = startLoading();
    await act(async () => {
      jest.advanceTimersByTime(300);
      setProgress(0.5, "best so far: 7 / 14");
    });
    expect(screen.getByTestId("loading-bar").props.accessibilityValue).toEqual({ min: 0, max: 100, now: 50 });
    expect(screen.getByText("best so far: 7 / 14")).toBeTruthy();
    done();
  });

  it("stands still under reduce-motion", async () => {
    setReducedMotionForTests(true);
    await render(<LoadingBar />);
    const done = startLoading();
    await act(async () => {
      jest.advanceTimersByTime(300);
    });
    expect(screen.getByTestId("loading-bar")).toBeTruthy();
    done();
  });
});

describe("Sheet and SidePanel (FR-063, FR-071)", () => {
  it("opens as a dialog named by its title and closes from its button", async () => {
    const onClose = jest.fn();
    const { rerender } = await render(
      <Sheet open onClose={onClose} title="Assign to" testID="sheet" footer={<Text>footer</Text>}>
        <Text>People first.</Text>
      </Sheet>,
    );
    expect(screen.getByTestId("sheet").props.accessibilityLabel).toBe("Assign to");
    expect(screen.getByText("People first.")).toBeTruthy();
    expect(screen.getByText("footer")).toBeTruthy();
    await fireEvent.press(screen.getByTestId("sheet-close"));
    expect(onClose).toHaveBeenCalledTimes(1);
    await rerender(
      <Sheet open={false} onClose={onClose} title="Assign to" testID="sheet">
        <Text>People first.</Text>
      </Sheet>,
    );
    // The surface stays mounted while it leaves, then goes.
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 250));
    });
    expect(screen.queryByTestId("sheet")).toBeNull();
  });

  it("leaves at once under reduce-motion", async () => {
    setReducedMotionForTests(true);
    const { rerender } = await render(
      <Sheet open onClose={() => undefined} title="Assign to" testID="sheet">
        <Text>x</Text>
      </Sheet>,
    );
    await rerender(
      <Sheet open={false} onClose={() => undefined} title="Assign to" testID="sheet">
        <Text>x</Text>
      </Sheet>,
    );
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
    });
    expect(screen.queryByTestId("sheet")).toBeNull();
  });

  it("becomes a sheet on a phone", async () => {
    const onClose = jest.fn();
    await render(
      <SidePanel open onClose={onClose} title="Details" testID="panel">
        <Text>Record</Text>
      </SidePanel>,
    );
    expect(screen.getByTestId("panel").props.accessibilityLabel).toBe("Details");
    await fireEvent.press(screen.getByTestId("panel-close"));
    expect(onClose).toHaveBeenCalled();
  });
});

const PEOPLE = [
  { id: "p1", label: "Dana Levi", secondary: "Gate 3", keywords: ["דנה לוי"] },
  { id: "p2", label: "Yosef Cohen", secondary: "Reception", keywords: ["יוסף כהן"] },
  { id: "p3", label: "נועם ברק", keywords: ["Noam Barak"] },
];

describe("SearchablePicker (FR-044)", () => {
  it("shows the chosen item, opens, filters by a Hebrew name and chooses", async () => {
    const onChange = jest.fn();
    await render(<SearchablePicker label="Employee" items={PEOPLE} value="p1" onChange={onChange} recentIds={["p2"]} testID="pick" />);
    expect(screen.getByText("Dana Levi")).toBeTruthy();
    expect(screen.getByText("Gate 3")).toBeTruthy();
    await fireEvent.press(screen.getByTestId("pick"));
    expect(screen.getByTestId("pick-sheet")).toBeTruthy();
    expect(screen.getByText("Recent")).toBeTruthy();
    await fireEvent.changeText(screen.getByTestId("pick-search"), "נועם");
    expect(screen.queryByTestId("pick-option-p1")).toBeNull();
    await fireEvent.press(screen.getByTestId("pick-option-p3"));
    expect(onChange).toHaveBeenCalledWith("p3");
  });

  it("says when nothing matches and shows an error", async () => {
    await render(<SearchablePicker label="Employee" items={PEOPLE} value={null} onChange={() => undefined} error="Choose someone." testID="pick" />);
    expect(screen.getByRole("alert")).toHaveTextContent(/Choose someone/);
    await fireEvent.press(screen.getByTestId("pick"));
    await fireEvent.changeText(screen.getByTestId("pick-search"), "zzz");
    expect(screen.getByText("Nothing matches.")).toBeTruthy();
  });
});

describe("Toast (FR-047)", () => {
  it("announces a notice, runs its action and leaves when dismissed", async () => {
    const onPress = jest.fn();
    await render(<ToastHost />);
    let id = 0;
    await act(async () => {
      id = showToast("Availability submitted", { action: { label: "Undo", onPress }, testID: "toast" });
    });
    expect(screen.getByTestId("toast").props.role).toBe("status");
    expect(screen.getByText("Availability submitted")).toBeTruthy();
    await fireEvent.press(screen.getByRole("button", { name: "Undo" }));
    expect(onPress).toHaveBeenCalled();
    await act(async () => {
      dismissToast(id);
      await new Promise((resolve) => setTimeout(resolve, 250));
    });
    expect(screen.queryByTestId("toast")).toBeNull();
  });

  it("keeps at most three notices", async () => {
    await render(<ToastHost />);
    await act(async () => {
      for (let i = 0; i < 5; i += 1) showToast(`Notice ${i}`, { testID: `toast-${i}`, tone: i % 2 ? "info" : "warning" });
    });
    expect(screen.queryByTestId("toast-0")).toBeNull();
    expect(screen.getByTestId("toast-4")).toBeTruthy();
  });
});

function Probe() {
  const theme = useTheme();
  const behind = useBackgroundColor();
  return (
    <>
      <Text testID="theme-name">{theme.name}</Text>
      <Text testID="behind">{behind}</Text>
      <Txt role="body" testID="txt">
        Body
      </Txt>
    </>
  );
}

describe("FeaturePanel, PageTitle, ClippedBox and Screen (FR-061)", () => {
  it("gives text on a panel the inverse tones and the panel's colour behind it", async () => {
    await render(
      <FeaturePanel testID="panel" bordered>
        <Probe />
      </FeaturePanel>,
    );
    expect(screen.getByTestId("panel")).toBeTruthy();
    expect(screen.getByTestId("behind")).toHaveTextContent("#101413");
    expect(screen.getByTestId("txt").props.style).toEqual(expect.arrayContaining([expect.objectContaining({ color: "#F5F3ED" })]));
  });

  it("titles the page with a level-one heading and its crumbs", async () => {
    await render(
      <PageTitle eyebrow={["HDS", "Design"]} title="Building blocks" testID="title">
        <Text>actions</Text>
      </PageTitle>,
    );
    expect(screen.getByText("HDS / Design")).toBeTruthy();
    const heading = screen.getByTestId("title");
    expect(heading.props.role).toBe("heading");
    expect(heading.props["aria-level"]).toBe(1);
    expect(screen.getByText("actions")).toBeTruthy();
  });

  it("cuts the trailing bottom corner too", async () => {
    await render(
      <ClippedBox clip={14} color="#D7A57E" corner="trailing-bottom" testID="box">
        <Text>x</Text>
      </ClippedBox>,
    );
    expect(screen.getByTestId("box").props.style).toEqual(expect.arrayContaining([expect.objectContaining({ backgroundColor: "#D7A57E" })]));
  });

  it("frames a screen on the canvas, scrolling or fixed", async () => {
    const { rerender } = await render(
      <ThemeProvider>
        <Screen testID="screen">
          <Probe />
        </Screen>
      </ThemeProvider>,
    );
    expect(screen.getByTestId("theme-name")).toHaveTextContent("light");
    expect(screen.getByTestId("behind")).toHaveTextContent("#DDE1DD");
    await rerender(
      <ThemeProvider>
        <Screen testID="screen" fixed>
          <Probe />
        </Screen>
      </ThemeProvider>,
    );
    expect(screen.getByTestId("screen")).toBeTruthy();
  });

  it("follows an override of the theme", async () => {
    function Switcher() {
      const theme = useTheme();
      return (
        <>
          <Text testID="name">{theme.name}</Text>
          <Text testID="dark" onPress={() => theme.setPreference("dark")}>
            dark
          </Text>
          <Text testID="compact" onPress={() => theme.setDensity("compact")}>
            compact
          </Text>
          <Text testID="density">{theme.density}</Text>
        </>
      );
    }
    await render(
      <ThemeProvider>
        <Switcher />
      </ThemeProvider>,
    );
    expect(screen.getByTestId("name")).toHaveTextContent("light");
    await fireEvent.press(screen.getByTestId("dark"));
    expect(screen.getByTestId("name")).toHaveTextContent("dark");
    await fireEvent.press(screen.getByTestId("compact"));
    expect(screen.getByTestId("density")).toHaveTextContent("compact");
  });
});

describe("Skeleton, EmptyState and Icon", () => {
  it("renders placeholders as one busy region and an empty state with its action", async () => {
    const onPress = jest.fn();
    await render(
      <>
        <SkeletonRows rows={2} testID="rows" />
        <Skeleton width={80} height={12} testID="block" />
        <EmptyState title="No shifts" body="Later." badge="Preview" feature="Feature 009" icon="calendar" action={{ label: "Open", onPress, testID: "act" }} />
        <Icon name="arrow-next" label="Next" testID="arrow" />
        <Icon name="clock" testID="clock" />
      </>,
    );
    expect(screen.getByTestId("rows").props.accessibilityLabel).toBe("Loading…");
    expect(screen.getByTestId("block", { includeHiddenElements: true })).toBeTruthy();
    expect(screen.getByText("Preview")).toBeTruthy();
    expect(screen.getByText("Feature 009")).toBeTruthy();
    await fireEvent.press(screen.getByTestId("act"));
    expect(onPress).toHaveBeenCalled();
    expect(screen.getByTestId("arrow").props.accessibilityLabel).toBe("Next");
    expect(screen.getByTestId("clock", { includeHiddenElements: true }).props.accessibilityElementsHidden).toBe(true);
  });

  it("holds the shimmer still under reduce-motion", async () => {
    setReducedMotionForTests(true);
    await render(<Skeleton testID="still" />);
    expect(screen.getByTestId("still", { includeHiddenElements: true }).props.children).toBeNull();
  });
});
