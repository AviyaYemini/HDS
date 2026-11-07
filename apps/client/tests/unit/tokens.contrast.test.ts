/**
 * Every text and icon colour pair in the design tokens meets WCAG contrast (specs/007-ux-redesign
 * FR-062, SC-008, task T008): 4.5:1 for text and 3:1 for icons in the light and dark themes, 7:1 and
 * 4.5:1 in the high-contrast themes.
 */
import base from "../../src/design/tokens/base.tokens.json";
import semantic from "../../src/design/tokens/semantic.tokens.json";
import { contrastFailures, contrastRatio } from "../../src/design/tokens/contrast";

describe("design-token contrast (FR-062)", () => {
  it("computes WCAG ratios", () => {
    expect(contrastRatio("#000000", "#FFFFFF")).toBeCloseTo(21, 5);
    expect(contrastRatio("#777777", "#FFFFFF")).toBeCloseTo(4.48, 2);
  });

  it("finds a pair under the minimum, so the check can fail", () => {
    const bad = {
      $extensions: { hds: { minimum: { light: { text: 4.5, icon: 3 } } } },
      theme: {
        light: {
          bg: { canvas: { $value: "{palette.white}" } },
          text: { faint: { $value: "{palette.grey}", $extensions: { hds: { on: ["bg.canvas"], kind: "text" } } } },
        },
      },
    };
    const palette = {
      palette: {
        white: { $type: "color", $value: { colorSpace: "srgb", components: [1, 1, 1], hex: "#FFFFFF" } },
        grey: { $type: "color", $value: { colorSpace: "srgb", components: [0.6, 0.6, 0.6], hex: "#999999" } },
      },
    };
    const failures = contrastFailures(bad, palette);
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({ theme: "light", token: "text.faint", on: "bg.canvas" });
  });

  it("passes every pair in all four themes", () => {
    const failures = contrastFailures(semantic, base).map(
      (f) => `${f.theme} ${f.token} on ${f.on}: ${f.ratio.toFixed(2)} < ${f.minimum}`,
    );
    expect(failures).toEqual([]);
  });

  it("declares every theme the spec names", () => {
    expect(Object.keys(semantic.theme).sort()).toEqual(["dark", "highContrastDark", "highContrastLight", "light"]);
  });
});
