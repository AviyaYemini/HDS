// Jest for the HDS client: the Expo preset, tests under tests/unit, and coverage on the logic.
module.exports = {
  preset: "jest-expo",
  testMatch: ["<rootDir>/tests/unit/**/*.test.ts?(x)"],
  setupFiles: ["<rootDir>/tests/unit/setup.ts"],
  collectCoverageFrom: ["src/**/*.{ts,tsx}", "!src/**/*.d.ts"],
  coverageThreshold: { global: { lines: 70 } },
};
