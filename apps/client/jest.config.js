// Jest for the HDS client: the Expo preset, tests under tests/unit, and coverage on the logic.
module.exports = {
  preset: "jest-expo",
  testMatch: ["<rootDir>/tests/unit/**/*.test.ts?(x)"],
  setupFiles: ["<rootDir>/tests/unit/setup.ts"],
  collectCoverageFrom: ["src/**/*.{ts,tsx}", "!src/**/*.d.ts"],
  coverageThreshold: { global: { lines: 70 } },
  // A margin for slow shared CI runners; locally every test takes well under a second.
  testTimeout: 15000,
  // Inside the app, so CI can keep the compiled files between runs.
  cacheDirectory: "<rootDir>/.jest-cache",
};
