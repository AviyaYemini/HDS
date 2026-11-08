/**
 * The root layout's start runs once (R72, a review comment on pull request 16): switching the
 * language afterwards must not take the kept session up again, nor ask a phone for biometrics a
 * second time.
 */
import { act, render, waitFor } from "@testing-library/react-native";

import RootLayout from "../../app/_layout";
import { session } from "../../src/api/default";
import { storeFor } from "../../src/api/tokens";
import { unlock } from "../../src/auth/biometric";
import { i18n } from "../../src/i18n";

jest.mock("expo-splash-screen", () => ({
  preventAutoHideAsync: jest.fn(async () => true),
  hideAsync: jest.fn(async () => true),
}));
jest.mock("../../src/auth/biometric", () => ({ unlock: jest.fn(async () => "unlocked") }));

afterEach(async () => {
  await storeFor("ios").clear();
  await i18n.changeLanguage("en");
});

it("starts once on a phone with a kept session, whatever the language does afterwards", async () => {
  await storeFor("ios").save({ access: "a-kept", refresh: "r-kept" });
  const restore = jest.spyOn(session, "restore").mockResolvedValue(true);
  try {
    await render(<RootLayout />);
    await waitFor(() => expect(restore).toHaveBeenCalledTimes(1));
    expect(unlock).toHaveBeenCalledTimes(1);

    await act(async () => {
      await i18n.changeLanguage("he");
    });
    await act(async () => {
      await i18n.changeLanguage("en");
    });

    expect(unlock).toHaveBeenCalledTimes(1);
    expect(restore).toHaveBeenCalledTimes(1);
  } finally {
    restore.mockRestore();
  }
});
