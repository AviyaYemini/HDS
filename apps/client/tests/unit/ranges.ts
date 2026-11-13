/**
 * Time ranges as the app prints them (the owner, 2026-11-13): "07:00–19:00" is held together with
 * word joiners and "+1" by a no-break space, so a narrow line never splits it. Tests keep writing
 * the plain form and pass it through `joined`, which spells it the printed way.
 */
export function joined(plain: string): string {
  return plain.replace(/(\d)–(\d)/g, "$1⁠–⁠$2").replace(/ \+(\d)/g, " +$1");
}

/**
 * The same, as a regular expression for a text match (special characters escaped). The testing
 * library folds the no-break space into a plain one, so either is accepted there.
 */
export function joinedPattern(plain: string): RegExp {
  return new RegExp(joined(plain).replace(/[.*+?^${}()|[\]\\]/g, "\\$&").replace(/\u00a0/g, "[\u00a0 ]"));
}
