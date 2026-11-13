/** A floating card beside a point, flipped and shifted to stay inside the window (the owner, 2026-11-13). */
import { placeCard } from "../../../src/design/components/placeCard";

const card = { width: 240, height: 120 };
const window = { width: 1440, height: 900 };

test("after and below the point when it fits", () => {
  expect(placeCard({ x: 600, y: 300 }, card, window)).toEqual({ left: 612, top: 312 });
});

test("flipped before the point at the window's far edge, and above it at the bottom", () => {
  expect(placeCard({ x: 1400, y: 850 }, card, window)).toEqual({ left: 1148, top: 718 });
});

test("shifted inside the window when neither side fits", () => {
  expect(placeCard({ x: 100, y: 50 }, card, { width: 300, height: 140 })).toEqual({ left: 8, top: 8 });
  expect(placeCard({ x: 0, y: 0 }, card, window)).toEqual({ left: 12, top: 12 });
});
