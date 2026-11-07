/**
 * The development gallery (specs/007-ux-redesign FR-067): every building block in every state, so
 * `tests/e2e/ux/a-design-system.spec.ts` can check them without data. Shown only while previews are
 * on; otherwise the address behaves like any unknown one.
 *
 * The [UI] session fills this screen (tasks T014 to T023). The test ids the spec reads are listed at
 * the top of that spec and are the contract between the two.
 */
import { Redirect } from "expo-router";

import { previewsEnabled } from "../../src/config/previews";

export default function Gallery() {
  if (!previewsEnabled) return <Redirect href="/" />;
  return null;
}
