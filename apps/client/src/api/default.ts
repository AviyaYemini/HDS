/** The app's one session, against `EXPO_PUBLIC_API_URL` (the local API by default). */
import { Platform } from "react-native";

import { Session } from "./session";
import { storeFor } from "./tokens";

/** Where the API is. */
export const API_URL = process.env.EXPO_PUBLIC_API_URL ?? "http://localhost:8000";

/** The session every screen uses. */
export const session = new Session(API_URL, storeFor(Platform.OS));
