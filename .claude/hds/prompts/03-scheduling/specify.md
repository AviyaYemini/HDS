A manager asks the system to generate a roster for a date range across all active projects at once, reviews the proposal, and accepts it as draft shifts. Use docs/hds-spec.md section 5 and docs/hds-api.md (Schedule generation, Scheduler contract), and original scenario B.

Hard rules: no overlapping shifts, minimum rest between shifts (default 11 hours, configurable), maximum weekly hours (default 48, configurable), respect blocked dates and absences. Goals: fill every slot, honour preferences, balance hours and night shifts across employees, avoid long runs of consecutive days, and minimise changes to an existing roster. Slots that cannot be filled are listed with the reason. The result says whether it is provably optimal or the best found within the time limit.

No technology details.
