/**
 * The app's one session, against `EXPO_PUBLIC_API_URL` (the local API by default). Every request it
 * sends is tracked, so the loading bar shows for a slow one (specs/007-ux-redesign FR-074).
 */
import { Platform } from "react-native";

import { reportReachable, reportUnreachable } from "../connectivity";
import { track } from "../design/loading/loadingStore";
import { Session } from "./session";
import { storeFor } from "./tokens";

/** Where the API is. */
export const API_URL = process.env.EXPO_PUBLIC_API_URL ?? "http://localhost:8000";

/** Send a request under the loading bar and tell the connectivity store how it went. */
async function send(request: Request): Promise<Response> {
  try {
    const response = await track(fetch(request));
    reportReachable();
    return response;
  } catch (failure) {
    reportUnreachable();
    throw failure;
  }
}

/** The session every screen uses. */
export const session = new Session(API_URL, storeFor(Platform.OS), send);
