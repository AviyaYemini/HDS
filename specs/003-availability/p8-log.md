# 003 Part 2: decision log

Decisions, review findings, mutation results and deviations for Part 2 (availability and manual
shifts), phases P8 to P13. Tests run on the private database `hds_p3_test`, never `hds_test`, and never
while another run uses it.

## Setup (T030)

Owner decisions of 2026-11-03, before implementation:

- Managers publish drafts; only Admins create, change or delete shifts and assignments.
- Absences (leave, reserve duty, sick) can be reported at any time; blocked, open and close follow the
  submission window.
- The Expo app lives in `apps/client` (the prompt's "apps/mobile" is read as this app).
- `POST /shifts/{id}/assignments` accepts an `Idempotency-Key` (FR-115a).
- An availability entry without a shift type covers every shift that date (FR-104).

Spec quality checklist: 32 of 32 items pass. `hds_p3_test` had no connections at the start.

## Group E: the pure rules (T031 to T036a)

- Failing first: every new test failed at import, because the package did not exist. The 31 first
  tests then passed on the first implementation.
- Decision: the warning type is `RuleWarning`, after review, not `Warning`. `validate` takes `focus`
  (the shifts being changed) and reports only warnings that involve one of them.
- Mutations on the first version, all caught (13): rest `<=`; touching ends overlap; Monday-start
  weeks; close not reported; night shift dated by its end; weekly limit inclusive; inactive not
  reported; weekday counted from Monday; shift type ignored; focus ignored; an impure import; DST with
  `fold=1`; under-headcount missed.
- ADR 009 written (T036a) and listed in `docs/README.md`.
- /code-review high, ten findings, all fixed, tests first. 7 new tests failed for the right reason:
  1. **A shift id given twice was counted twice (fixed):** for example a changed shift with its stored
     copy. `validate` refuses a duplicate id; the service must pass only the new version.
  2. **`local_span` could end before it starts (fixed):** an end inside the spring gap, or an end
     before the start without crossing midnight. It now raises `ValueError`; the service will turn it
     into a refusal.
  3. **Rest was judged between non-neighbours (fixed):** a shift two places later was reported again.
     Rest is now judged only between a shift and the next one, with no other shift starting in
     between. The property test's reference changed the same way; that is the rule changing, by
     review, not a test weakened.
  4. **A missing employee was treated as active (fixed):** it now raises `KeyError`.
  5. **An unknown entry kind was ignored (fixed):** the kind is a `Literal`, and other values raise
     `ValueError`.
  6. **`Warning` shadowed the built-in (fixed):** renamed `RuleWarning`.
  7. and 8. **Week counting repeated per shift, and the weekday convention duplicated (fixed):** each
     shift's week is computed once, and one `_sunday_index` helper serves both.
  9. **The data model's description of the inputs was stale (fixed).**
  10. **The purity test let `..` imports through (fixed):** only imports from the package itself pass.
- Mutations on the final version, all caught (19). E6 (weekly limit inclusive) was caught only by the
  property test's random examples, so `test_exactly_the_weekly_maximum_is_allowed` was added; E6 is
  now caught by it. New mutations E14 to E19 cover findings 1 to 4 and 10.
- /hds-review: no violations. The package imports only the standard library (checked by two tests and
  the hook). No endpoint or API change yet.
- Final gates: all green; full suite 1564 passed; concurrency 36 passed three times.
