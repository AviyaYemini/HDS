/**
 * The generated theme matches the tokens (specs/007-ux-redesign FR-061, research R3, task T016):
 * `theme.generated.ts` is exactly what the generator produces from the three token files, and the
 * values the design language fixes come through.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";

import base from "../../src/design/tokens/base.tokens.json";
import component from "../../src/design/tokens/component.tokens.json";
import semantic from "../../src/design/tokens/semantic.tokens.json";
import { generateTheme, resolveValue } from "../../src/design/theme/generate";
import { component as built, motion, themes } from "../../src/design/theme/theme.generated";

type Json = { [key: string]: unknown };
const files = { base: base as Json, semantic: semantic as Json, component: component as Json };

describe("theme generation (FR-061)", () => {
  it("is committed exactly as generated (no drift)", () => {
    const committed = readFileSync(join(__dirname, "../../src/design/theme/theme.generated.ts"), "utf8");
    expect(committed).toBe(generateTheme(files));
  });

  it("resolves references, colours, dimensions and durations", () => {
    expect(resolveValue("{palette.copper-700}", files, files.base)).toBe("#834B30");
    expect(resolveValue("{size.64}", files, files.component)).toBe(64);
    expect(resolveValue({ value: 300, unit: "ms" }, files, files.component)).toBe(300);
    expect(() => resolveValue("{palette.nothing}", files, files.base)).toThrow(/unresolved/);
  });

  it("carries Option B's values into the themes", () => {
    expect(themes.light.bg.canvas).toBe("#DDE1DD");
    expect(themes.light.accent.default).toBe("#834B30");
    expect(themes.light.cta.bg).toBe("#D7A57E");
    expect(themes.light.inverse.bg).toBe("#101413");
    expect(themes.dark.bg.canvas).toBe("#101413");
    expect(Object.keys(themes).sort()).toEqual(["dark", "highContrastDark", "highContrastLight", "light"]);
    expect(built.clip.panel).toBe(20);
    expect(built.clip.cta).toBe(14);
    expect(built.loadingBar).toEqual({ height: 3, showAfter: 300, minVisible: 400 });
    expect(motion.duration).toEqual({ fast: 120, base: 200, slow: 320, none: 0 });
    expect(motion.easing.standard).toEqual([0.2, 0, 0, 1]);
  });
});
