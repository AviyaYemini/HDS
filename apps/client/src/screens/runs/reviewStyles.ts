/**
 * The styles the proposal review, its empty-slot reasons and its slot blocks share.
 */
import { StyleSheet } from "react-native";

export const styles = StyleSheet.create({
  lockMark: { flexDirection: "row", alignItems: "center", gap: 4 },
  option: { gap: 2 },
  weeks: { gap: 16 },
  bar: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 8 },
  week: { gap: 12 },
  weekRow: { flexDirection: "row", alignItems: "flex-start", gap: 8 },
  column: { flexGrow: 1, flexShrink: 1, flexBasis: 0, minWidth: 0, gap: 8, paddingTop: 8, borderTopWidth: 1 },
  dayStack: { gap: 8, paddingTop: 8, borderTopWidth: 1 },
  block: { gap: 4, padding: 8, borderWidth: 1 },
  person: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 4 },
  removed: { textDecorationLine: "line-through" },
  break: { gap: 4 },
  panel: { gap: 8 },
});
