// Jest's resolver, with the "import" export condition for the ESM-only packages the app uses
// (@hebcal/* and the packages they depend on). Every other request resolves as Jest would on its
// own, so CommonJS entries such as i18next's stay in use.
const ESM_ONLY = /^(@hebcal\/|temporal-polyfill|temporal-spec|temporal-utils|quick-lru)/;

module.exports = (request, options) => {
  if (ESM_ONLY.test(request)) {
    const conditions = ["import", "default", ...(options.conditions ?? [])];
    return options.defaultResolver(request, { ...options, conditions });
  }
  return options.defaultResolver(request, options);
};
