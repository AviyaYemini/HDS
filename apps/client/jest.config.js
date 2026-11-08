// Jest for the HDS client: the Expo preset, tests under tests/unit, and coverage on the logic.
module.exports = {
  preset: "jest-expo",
  testMatch: ["<rootDir>/tests/unit/**/*.test.ts?(x)"],
  setupFiles: ["<rootDir>/tests/unit/setup.ts"],
  // jest-expo's list, plus the ESM-only packages the locale plumbing uses (specs/007 T017), so
  // Babel turns them into CommonJS for Jest.
  transformIgnorePatterns: [
    "/node_modules/(?!(.pnpm|react-native|@react-native|@react-native-community|expo|@expo|@expo-google-fonts|react-navigation|@react-navigation|@sentry/react-native|native-base|standard-navigation|@formatjs|@hebcal|temporal-polyfill|temporal-spec|temporal-utils|quick-lru))",
    "/node_modules/react-native-reanimated/plugin/",
    "/node_modules/@react-native/babel-preset/",
  ],
  collectCoverageFrom: ["src/**/*.{ts,tsx}", "!src/**/*.d.ts"],
  coverageThreshold: { global: { lines: 70 } },
  // @hebcal/* and their dependencies publish ESM only (an "import" condition and nothing else); the resolver accepts that
  // condition for them alone, and Babel turns them into CommonJS through transformIgnorePatterns.
  resolver: "<rootDir>/tests/unit/resolver.js",
  // A margin for slow shared CI runners; locally every test takes well under a second.
  testTimeout: 15000,
  // Inside the app, so CI can keep the compiled files between runs.
  cacheDirectory: "<rootDir>/.jest-cache",
};
