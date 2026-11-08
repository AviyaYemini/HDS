# Review ledger: 008 auto-scheduler review

Every finding of a review of this feature (/code-review, /hds-review, the reviewer session), one
row each. Status is `open`, `fixed` or `won't-fix`; a won't-fix row says why in the finding. The
commit that fixes a row names its id in its message, for example `fix(api): … (R12)`, so
`git log --grep R12` finds it. A group is done only when no row is open. /hds-status shows the
count.

| ID | Source | Finding | Status |
| --- | --- | --- | --- |
