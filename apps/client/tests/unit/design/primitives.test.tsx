/**
 * The primitives' roles, names and states (specs/007-ux-redesign FR-063, FR-064, task T023):
 * buttons, chips, the segmented control, text fields and the table through React Native Testing
 * Library, in English (the strings file is the only source of text).
 */
import { fireEvent, render, screen } from "@testing-library/react-native";

import {
  Button,
  EmptyState,
  InlineBanner,
  SegmentedControl,
  StatusChip,
  Table,
  TextField,
  clippedPoints,
} from "../../../src/design/components";

describe("Button", () => {
  it("is a button named by its label and presses", async () => {
    const onPress = jest.fn();
    await render(<Button label="Publish roster" onPress={onPress} testID="b" />);
    const button = screen.getByRole("button", { name: "Publish roster" });
    await fireEvent.press(button);
    expect(onPress).toHaveBeenCalledTimes(1);
  });

  it("stays legible when disabled and says why (FR-008)", async () => {
    const onPress = jest.fn();
    await render(<Button label="Publish roster" onPress={onPress} disabled disabledReason="Two seats are still unfilled." testID="b" />);
    expect(screen.getByRole("button", { name: "Publish roster", disabled: true })).toBeTruthy();
    expect(screen.getByTestId("b-reason")).toHaveTextContent("Two seats are still unfilled.");
    await fireEvent.press(screen.getByTestId("b"));
    expect(onPress).not.toHaveBeenCalled();
  });

  it("keeps its label while busy and ignores presses", async () => {
    const onPress = jest.fn();
    await render(<Button label="Saving" onPress={onPress} busy testID="b" />);
    expect(screen.getByRole("button", { name: /Saving/, busy: true })).toBeTruthy();
    await fireEvent.press(screen.getByTestId("b"));
    expect(onPress).not.toHaveBeenCalled();
  });
});

describe("ClippedBox", () => {
  it("cuts the leading top corner, mirrored in Hebrew", async () => {
    expect(clippedPoints(100, 40, 20, false)).toBe("20,0 100,0 100,40 0,40 0,20");
    expect(clippedPoints(100, 40, 20, true)).toBe("0,0 80,0 100,20 100,40 0,40");
  });
});

describe("StatusChip", () => {
  it("names the status in words", async () => {
    await render(<StatusChip status="success" label="Accepted" testID="chip" />);
    expect(screen.getByLabelText("Accepted")).toBeTruthy();
    expect(screen.getByText("Accepted")).toBeTruthy();
  });
});

describe("SegmentedControl", () => {
  it("is a radio group whose chosen segment is checked", async () => {
    const onChange = jest.fn();
    await render(
      <SegmentedControl
        label="View"
        options={[
          { value: "hours", label: "Hours" },
          { value: "pay", label: "Pay" },
        ]}
        value="hours"
        onChange={onChange}
      />,
    );
    expect(screen.getByLabelText("View").props.role).toBe("radiogroup");
    expect(screen.getByRole("radio", { name: "Hours", checked: true })).toBeTruthy();
    await fireEvent.press(screen.getByRole("radio", { name: "Pay" }));
    expect(onChange).toHaveBeenCalledWith("pay");
  });
});

describe("TextField", () => {
  it("is labelled, shows helper text, and announces an error (FR-045)", async () => {
    const { rerender } = await render(<TextField label="Full name" value="" onChangeText={() => undefined} helper="As on the ID." testID="f" />);
    expect(screen.getByLabelText("Full name")).toBeTruthy();
    expect(screen.getByTestId("f-helper")).toHaveTextContent("As on the ID.");
    await rerender(<TextField label="Full name" value="" onChangeText={() => undefined} error="Write a full name." testID="f" />);
    expect(screen.getByRole("alert")).toHaveTextContent("Write a full name.");
  });

  it("reports typing", async () => {
    const onChangeText = jest.fn();
    await render(<TextField label="Full name" value="" onChangeText={onChangeText} testID="f" />);
    await fireEvent.changeText(screen.getByTestId("f"), "Dana");
    expect(onChangeText).toHaveBeenCalledWith("Dana");
  });
});

interface Row {
  id: string;
  name: string;
  hours: string;
}
const rows: Row[] = [
  { id: "1", name: "Dana", hours: "38.5" },
  { id: "2", name: "Yosef", hours: "42" },
];
const columns = [
  { key: "name", title: "Name", sortable: true },
  { key: "hours", title: "Hours", numeric: true },
];

describe("Table", () => {
  it("has a caption, headers and cells, and sorts on request", async () => {
    const onSort = jest.fn();
    await render(<Table<Row> caption="Staff" columns={columns} rows={rows} keyOf={(row) => row.id} sort={{ key: "name", direction: "asc" }} onSort={onSort} />);
    expect(screen.getByLabelText("Staff")).toBeTruthy();
    expect(screen.getByText("Dana")).toBeTruthy();
    expect(screen.getByText("42")).toBeTruthy();
    await fireEvent.press(screen.getByRole("button", { name: "Name" }));
    expect(onSort).toHaveBeenCalledWith("name", "desc");
  });

  it("shows placeholders on the first load, never the empty state (FR-047)", async () => {
    await render(<Table<Row> caption="Staff" columns={columns} rows={[]} keyOf={(row) => row.id} loading empty={<EmptyState title="Nobody yet" />} testID="t" />);
    expect(screen.getByTestId("t-loading")).toBeTruthy();
    expect(screen.queryByText("Nobody yet")).toBeNull();
  });

  it("shows the empty state when there is nothing", async () => {
    await render(<Table<Row> caption="Staff" columns={columns} rows={[]} keyOf={(row) => row.id} empty={<EmptyState title="Nobody yet" body="Invite staff." />} />);
    expect(screen.getByText("Nobody yet")).toBeTruthy();
  });
});

describe("InlineBanner", () => {
  it("is an alert when it warns and a status otherwise", async () => {
    const { rerender } = await render(<InlineBanner tone="warning" title="The roster changed." testID="b" />);
    expect(screen.getByTestId("b").props.role).toBe("alert");
    await rerender(
      <InlineBanner tone="info" testID="b">
        All saved.
      </InlineBanner>,
    );
    expect(screen.getByTestId("b").props.role).toBe("status");
    expect(screen.getByText("All saved.")).toBeTruthy();
  });
});
