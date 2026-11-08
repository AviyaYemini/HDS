/**
 * Biometric unlock on phones (specs/007-ux-redesign FR-010, E15; ADR 016): a convenience gate in
 * front of the kept refresh token, not a security boundary. Where the device has biometrics
 * enrolled the app asks for them before it takes the kept session up; where it has none, or on
 * the web, nothing is asked. No biometric data leaves the device.
 */
import * as LocalAuthentication from "expo-local-authentication";
import { Platform } from "react-native";

/** What the gate decided. */
export type UnlockResult = "unlocked" | "cancelled" | "unavailable";

/** Whether this device can ask for biometrics. */
export async function biometricsAvailable(): Promise<boolean> {
  if (Platform.OS === "web") return false;
  try {
    return (await LocalAuthentication.hasHardwareAsync()) && (await LocalAuthentication.isEnrolledAsync());
  } catch {
    return false;
  }
}

/** Ask the device; `unavailable` lets the app continue without a gate. */
export async function unlock(promptMessage: string, cancelLabel: string): Promise<UnlockResult> {
  if (!(await biometricsAvailable())) return "unavailable";
  try {
    const result = await LocalAuthentication.authenticateAsync({ promptMessage, cancelLabel, disableDeviceFallback: false });
    return result.success ? "unlocked" : "cancelled";
  } catch {
    return "unavailable";
  }
}
