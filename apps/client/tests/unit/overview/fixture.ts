/** A `GET /api/v1/reports/overview` answer shaped like the demo plan's contract. Made-up names. */
import type { Overview } from "../../../src/screens/overview/model";

export const OVERVIEW: Overview = {
  from: "2026-11-01",
  to: "2026-11-30",
  today: "2026-11-13",
  billing_actual: "219860.00",
  billing_forecast: "480200.00",
  pay_actual: "178560.00",
  pay_forecast: "393764.00",
  margin_pct: "18.00",
  hours_actual: "1984.00",
  hours_forecast: "4310.00",
  coverage: { week_start: "2026-11-08", filled: 96, needed: 100, empty_places: 4, projects_with_gaps: 2 },
  coverage_next: { week_start: "2026-11-15", filled: 90, needed: 100, empty_places: 10, projects_with_gaps: 3 },
  people: { active: 14, working_this_week: 11, temporary: 3, temporary_ending_soon: 1, near_weekly_limit: 2 },
  days: [],
  weeks: [
    { week_start: "2026-11-01", hours: "980.00", billing: "108000.00", pay: "88500.00", forecast: false },
    { week_start: "2026-11-08", hours: "1004.00", billing: "111860.00", pay: "90060.00", forecast: true },
  ],
  projects: [
    { id: "p1", name: "Harbour gate", client_name: "Alpha Security", managers: ["Dana L."], type: "security", hours: "868.00", billing: "95000.00", pay: "75000.00", margin_pct: "21.00", coverage_pct: "98.00", empty_places: 0, temporary_workers: 1, terms_estimated: false, status: "ok", empty_places_next: 0, weeks: [] },
    { id: "p2", name: "Seaside mall", client_name: "Beta Guarding", managers: ["Omer K."], type: "security", hours: "372.00", billing: "40000.00", pay: "36400.00", margin_pct: "9.00", coverage_pct: "89.00", empty_places: 2, temporary_workers: 0, terms_estimated: true, status: "gaps", empty_places_next: 3, weeks: [] },
    { id: "p3", name: "Training · course 7", client_name: "Alpha Security", managers: [], type: "training", hours: "228.00", billing: "20000.00", pay: "20800.00", margin_pct: "-4.00", coverage_pct: null, empty_places: 0, temporary_workers: 0, terms_estimated: false, status: "loss", empty_places_next: 0, weeks: [] },
  ],
  terms_estimated: true,
  previous: { from: "2026-10-01", to: "2026-10-31", hours: "4100.00", billing: "452000.00", pay: "371000.00", margin_pct: "17.92", hours_to_date: "1800.00", billing_to_date: "201000.00", hours_to_rostered: "3000.00", billing_to_rostered: "330000.00" },
};
