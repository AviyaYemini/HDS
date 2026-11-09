/**
 * The availability grid's touch feedback on a device (specs/007-ux-redesign FR-016): a light tap on a
 * mark, a success on submit; none on the web.
 */
import * as Haptics from "expo-haptics";
import { Platform } from "react-native";

export function lightTap(): void {
  if (Platform.OS !== "web") void Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light).catch(() => undefined);
}

export function successTap(): void {
  if (Platform.OS !== "web") void Haptics.notificationAsync(Haptics.NotificationFeedbackType.Success).catch(() => undefined);
}
