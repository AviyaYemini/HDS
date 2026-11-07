/**
 * Motion tokens (specs/007-ux-redesign FR-070, research R14, task T009): three durations of 120, 200
 * and 320 ms and easings with no overshoot, which would read as bounce against Option B's square
 * shapes.
 */
import base from "../../src/design/tokens/base.tokens.json";
import component from "../../src/design/tokens/component.tokens.json";

describe("motion tokens (FR-070)", () => {
  it("has the three durations", () => {
    const { fast, base: middle, slow } = base.motion.duration;
    expect([fast.$value, middle.$value, slow.$value]).toEqual([
      { value: 120, unit: "ms" },
      { value: 200, unit: "ms" },
      { value: 320, unit: "ms" },
    ]);
  });

  it("never overshoots: every control point stays within 0 and 1", () => {
    for (const [name, easing] of Object.entries(base.motion.easing)) {
      expect(easing.$type).toBe("cubicBezier");
      for (const point of easing.$value) {
        expect({ name, point, inside: point >= 0 && point <= 1 }).toEqual({ name, point, inside: true });
      }
    }
  });

  it("builds every component transition from the motion tokens", () => {
    for (const transition of Object.values(component.motion)) {
      expect(transition.$value.duration).toMatch(/^\{motion\.duration\.(fast|base|slow)\}$/);
      expect(transition.$value.timingFunction).toMatch(/^\{motion\.easing\.(standard|emphasized)\}$/);
    }
  });
});
