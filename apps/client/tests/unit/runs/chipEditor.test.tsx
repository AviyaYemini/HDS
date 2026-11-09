/**
 * The run's controls and chips (specs/008-auto-scheduler-review FR-017, FR-020): each chip reads in
 * words and can be removed, a control changes what it says, and a new chip starts from an empty
 * builder that can be cancelled.
 */
import { act, fireEvent, screen } from "@testing-library/react-native";

import { ChipEditor } from "../../../src/screens/runs/ChipEditor";
import { DEFAULT_CONTROLS, type Chip } from "../../../src/screens/runs/steering";
import { FakeApi, json, renderWith, signedIn } from "../fakeApi";

const ANN = "44444444-4444-4444-8444-444444444444";
const PEOPLE = { items: [{ id: ANN, full_name: "Ann", role: "employee" }, { id: "c", full_name: "Client", role: "client" }], next_cursor: null };

test("chips read in words and leave, controls change, and the builder opens and cancels", async () => {
  const api = new FakeApi().on("GET", "/api/v1/employees", json(200, PEOPLE)).on("GET", "/api/v1/shift-types", json(200, { items: [] }));
  const session = await signedIn(api);
  const onChips = jest.fn();
  const onControls = jest.fn();
  const chips: Chip[] = [{ kind: "max_nights", user_id: ANN, count: 2 } as Chip];
  await renderWith(session, <ChipEditor controls={DEFAULT_CONTROLS} onControls={onControls} chips={chips} onChips={onChips} projects={[]} />);

  expect(await screen.findByText("Ann: at most 2 nights")).toBeTruthy();
  await act(async () => {
    fireEvent.press(screen.getByRole("button", { name: "Remove: Ann: at most 2 nights" }));
  });
  expect(onChips).toHaveBeenCalledWith([]);

  await act(async () => {
    fireEvent.press(screen.getByText("Keep changes small"));
  });
  expect(onControls).toHaveBeenCalledWith({ ...DEFAULT_CONTROLS, keep_changes_small: true });

  await act(async () => {
    fireEvent.press(screen.getByTestId("action-chip-new"));
  });
  expect(screen.getByTestId("chip-builder")).toBeTruthy();
  await act(async () => {
    fireEvent.press(screen.getByTestId("action-chip-cancel"));
  });
  expect(screen.queryByTestId("chip-builder")).toBeNull();
});
