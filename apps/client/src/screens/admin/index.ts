/**
 * The admin screens on the one table and form pattern (specs/007-ux-redesign FR-043 to FR-049, T083
 * to T085): staff, clients, sites, projects, shift templates and special days as tables with a
 * record panel; staffing needs and pay settings as forms. Deleting shows only for whoever may (an
 * Admin); a Manager creates and edits.
 * One file per screen; the routes import them from here.
 */
export { ClientsScreen } from "./ClientsScreen";
export { PaySettingsScreen } from "./PaySettingsScreen";
export { ProjectsScreen } from "./ProjectsScreen";
export { ShiftTemplatesScreen } from "./ShiftTemplatesScreen";
export { SitesScreen } from "./SitesScreen";
export { SpecialDaysScreen } from "./SpecialDaysScreen";
export { StaffingNeedsScreen } from "./StaffingNeedsScreen";
export { StaffScreen } from "./StaffScreen";
