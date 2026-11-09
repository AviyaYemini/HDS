/**
 * The styles the Account screen and its sections share.
 */
import { StyleSheet } from "react-native";

import { component } from "../../design/theme/theme.generated";

export const styles = StyleSheet.create({
  section: { gap: 14, paddingTop: 20, borderTopWidth: 1 },
  form: { gap: 14, maxWidth: 560 },
  row: { flexDirection: "row", flexWrap: "wrap", gap: 12, alignItems: "center" },
  radios: { flexDirection: "row", borderWidth: 1, alignSelf: "flex-start" },
  radio: { paddingHorizontal: 18, justifyContent: "center", borderTopWidth: component.header.activeRule },
  sessionRow: { flexDirection: "row", alignItems: "center", gap: 12, paddingVertical: 10, borderBottomWidth: 1 },
  sessionText: { flex: 1, gap: 2 },
  hoursRow: { gap: 8, paddingVertical: 10, borderBottomWidth: 1 },
  daySwitch: { flexDirection: "row", alignItems: "center", gap: 12, minHeight: component.target.phone },
  dayName: { flex: 1 },
  switchTrack: { width: 40, height: 22, borderWidth: 1, padding: 2, justifyContent: "center" },
  switchThumb: { width: 16, height: 16 },
  hoursFields: { flexDirection: "row", gap: 12 },
  timeField: { flex: 1, maxWidth: 160 },
});
