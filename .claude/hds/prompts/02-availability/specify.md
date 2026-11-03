Employees tell the company when they can and cannot work, and managers/admins can build and publish shifts by hand. Use docs/hds-spec.md section 4 (AVL-*, EMP-1, manual SCH rows) and docs/hds-api.md (Availability, Shifts and assignments).

Availability types: blocked, open, close, and absence (leave, reserve duty, sick). Open means the employee would like to work the shift and close means they would rather not; both are preferences, not blocks. Employees edit their own availability inside a submission window set by managers; admins can edit anyone's. Shift templates define the shift types per project. Admins create, edit and delete shifts and assignments; the system shows warnings (for example too little rest between shifts) but does not block the admin. Published shifts are visible to employees, who see only their own assignments, never co-workers'. Drafts are visible to managers and admins only.

No technology details. Out of scope: automatic schedule generation.
