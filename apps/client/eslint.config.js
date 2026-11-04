// ESLint for the HDS client: Expo's base config plus a rule that refuses literal strings in JSX,
// so every user-facing text goes through i18n (Constitution VIII).
const { defineConfig } = require("eslint/config");
const expoConfig = require("eslint-config-expo/flat");
const i18next = require("eslint-plugin-i18next");

module.exports = defineConfig([
  expoConfig,
  {
    ignores: ["dist/*", "coverage/*", ".expo/*", "playwright-report/*", "test-results/*"],
  },
  {
    files: ["app/**/*.tsx", "src/**/*.tsx"],
    plugins: { i18next },
    rules: {
      // Text children and the attributes people read or hear; ids, routes and roles are not text.
      "i18next/no-literal-string": [
        "error",
        {
          mode: "jsx-only",
          "jsx-attributes": {
            include: ["label", "title", "placeholder", "accessibilityLabel", "accessibilityHint", "aria-label", "alt"],
          },
        },
      ],
    },
  },
]);
