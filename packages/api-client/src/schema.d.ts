// Generated from the HDS OpenAPI document by packages/api-client/scripts/generate.mjs.
// Do not edit: run `pnpm api-client:generate` after changing the API.

export interface paths {
    "/api/v1/assignments/{id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post?: never;
        /**
         * Remove an assignment
         * @description A Manager of the project, or an Admin. Take the employee off the shift; the answer lists the warnings the shift has left, for example too few people. A stale `If-Match` is refused with 412 (`roster.changed`).
         */
        delete: operations["unassign_api_v1_assignments__id__delete"];
        options?: never;
        head?: never;
        /**
         * Change a person's own times in a shift
         * @description A Manager of the project, or an Admin. Set the person's own `starts_at` and `ends_at` in the shift, at most 4 hours before it starts and after it ends (422 `assignment.times_out_of_range`), or both null for the shift's times (007 FR-027). Pay, the rule checks and reports use the person's times. A stale `If-Match` is refused with 412 (`roster.changed`).
         */
        patch: operations["change_assignment_times_api_v1_assignments__id__patch"];
        trace?: never;
    };
    "/api/v1/assignments/{id}/overrides": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Keep an assignment despite a warning
         * @description A Manager of the project, or an Admin. Record why the assignment stays despite one of its current warnings; the publish review then no longer counts that warning as open. A code the assignment does not have now is refused with 422 (`override.warning_not_found`), a second override of the same warning with 409 (`override.duplicate`). Hard stops have no override.
         */
        post: operations["override_api_v1_assignments__id__overrides_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/availability-windows": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List submission windows
         * @description Employee, Manager or Admin. Return the windows that share a date with `from` to `to` (both optional, inclusive), in date order, each with whether it is still open.
         */
        get: operations["list_windows_api_v1_availability_windows_get"];
        put?: never;
        /**
         * Open a submission window
         * @description Manager or Admin. Open a window for a period of dates with a deadline. A period that ends before it starts is refused with 422 (`windows.date_range_invalid`); one that shares a date with another window with 409 (`windows.overlap`).
         */
        post: operations["open_window_api_v1_availability_windows_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/availability-windows/{id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post?: never;
        /**
         * Remove a window
         * @description Manager or Admin. Remove a window; the entries submitted in it stay.
         */
        delete: operations["remove_window_api_v1_availability_windows__id__delete"];
        options?: never;
        head?: never;
        /**
         * Change a window's deadline
         * @description Manager or Admin. Move the deadline of a window.
         */
        patch: operations["change_window_api_v1_availability_windows__id__patch"];
        trace?: never;
    };
    "/api/v1/availability-windows/{id}/submissions": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Track who has submitted availability
         * @description Manager or Admin. Every active employee, manager and admin with their availability status for the window, and after the deadline whether they count as unavailable for not submitting.
         */
        get: operations["tracker_api_v1_availability_windows__id__submissions_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/clients": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List clients
         * @description Manager or Admin. Return the organisation's non-deleted clients, optionally only active or only inactive ones.
         */
        get: operations["list_clients_api_v1_clients_get"];
        put?: never;
        /**
         * Create a client
         * @description Manager or Admin. Create a client with its contract start date and optional contact details. A contract end before its start returns 422 (`clients.date_range_invalid`).
         */
        post: operations["create_client_api_v1_clients_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/clients/{id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get a client
         * @description Manager or Admin. Return one client of the organisation.
         */
        get: operations["get_client_api_v1_clients__id__get"];
        put?: never;
        post?: never;
        /**
         * Delete a client
         * @description Admin only. Soft-delete a client, deactivate its portal users and end their sessions. Refused with 409 (`clients.has_projects`) while the client still has projects.
         */
        delete: operations["delete_client_api_v1_clients__id__delete"];
        options?: never;
        head?: never;
        /**
         * Edit a client
         * @description Manager or Admin. Update contact details, contract dates or active state. Deactivating an active client also deactivates its portal users and ends their sessions; reactivating the client does not reactivate them. An explicit null clears a field that allows it and is refused with 422 on a required one; an omitted field is left alone. If either date changes, the pair as it will be after the change (a date not sent keeps its stored value) must not end before it starts, else 422 (`clients.date_range_invalid`); a pair sent back unchanged is not judged.
         */
        patch: operations["edit_client_api_v1_clients__id__patch"];
        trace?: never;
    };
    "/api/v1/clients/{id}/users": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List a client's users
         * @description Manager or Admin. Return the portal accounts attached to the client.
         */
        get: operations["list_client_users_api_v1_clients__id__users_get"];
        put?: never;
        /**
         * Create a client user
         * @description Manager or Admin. Create a portal account with the Client role for this client, with an initial password of at least 12 characters. A duplicate email returns 409 (`users.duplicate_email`); an inactive client returns 409 (`users.client_inactive`). Answers 503 (`service.busy`) with `Retry-After` when the password-hashing queue is full; nothing is changed.
         */
        post: operations["create_client_user_api_v1_clients__id__users_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/clients/{id}/users/{user_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post?: never;
        /**
         * Remove a client user
         * @description Manager or Admin. Soft-delete a portal account of this client, ending its sessions and cancelling its links. Staff accounts cannot be removed here and return 404.
         */
        delete: operations["remove_client_user_api_v1_clients__id__users__user_id__delete"];
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/constraints/{id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post?: never;
        /**
         * Remove an availability entry
         * @description As adding one: the window rule applies to the entry's dates.
         */
        delete: operations["remove_entry_api_v1_constraints__id__delete"];
        options?: never;
        head?: never;
        /**
         * Change an availability entry
         * @description As adding one: the window rule applies to the dates the entry has and the dates it gets. An explicit null clears `absence_kind`, `shift_type_id` or `weekday`.
         */
        patch: operations["change_entry_api_v1_constraints__id__patch"];
        trace?: never;
    };
    "/api/v1/employees": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List employees
         * @description Manager or Admin. Search the organisation's non-deleted users by name or email and filter by role, active state, group or qualification. All filters combine with AND and the result is ordered by name. A group or qualification from another organisation, or one that does not exist, returns 404.
         */
        get: operations["list_employees_api_v1_employees_get"];
        put?: never;
        /**
         * Create an employee
         * @description Manager or Admin. Create an account with an initial password of at least 12 characters. The role defaults to employee; only an Admin may create an Admin, and Client users have their own endpoints (`users.role_invalid`). A duplicate email returns 409 (`users.duplicate_email`). Answers 503 (`service.busy`) with `Retry-After` when the password-hashing queue is full; nothing is changed.
         */
        post: operations["create_employee_api_v1_employees_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/employees/{id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get an employee
         * @description Manager or Admin for any user in the organisation; every other role only for their own id (otherwise 403 `users.forbidden`).
         */
        get: operations["get_employee_api_v1_employees__id__get"];
        put?: never;
        post?: never;
        /**
         * Delete an employee
         * @description Admin only. Soft-delete the user: history is kept and the email can be reused. Ends the user's sessions. The last active Admin cannot be deleted (409 `users.last_admin`).
         */
        delete: operations["delete_employee_api_v1_employees__id__delete"];
        options?: never;
        head?: never;
        /**
         * Edit an employee
         * @description Manager or Admin. Update name, phone, hourly rate or role. Only an Admin may change an Admin or assign the Admin role. Changing a role ends the user's sessions. Demoting the last active Admin returns 409 (`users.last_admin`). An explicit null clears a field that allows it and is refused with 422 on a required one; an omitted field is left alone. Changing the role of a Client account returns 409 (`users.client_role_locked`).
         */
        patch: operations["edit_employee_api_v1_employees__id__patch"];
        trace?: never;
    };
    "/api/v1/employees/{id}/availability-submissions": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Submit an employee's availability on their behalf
         * @description Admin. Submit the employee's current blocked, open and close entries for `window_id`, at any time, the deadline included. The submission records the Admin as `submitted_by`. This is the explicit way to count an employee who did not submit; an Admin's edit of their entries alone never creates a submission.
         */
        post: operations["submit_on_behalf_api_v1_employees__id__availability_submissions_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/employees/{id}/constraints": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List an employee's availability
         * @description The employee themself, a Manager or an Admin. Return the entries that share a date with `from` to `to` (both optional). Another employee's, asked by an Employee, is 404.
         */
        get: operations["list_entries_api_v1_employees__id__constraints_get"];
        put?: never;
        /**
         * Add an availability entry
         * @description The employee themself inside an open window (absences any time), or an Admin at any time. A Manager adding to another employee's is refused with 403 (`availability.admin_required`). Dates outside a window answer 409 (`availability.no_window`), a passed deadline 409 (`availability.window_closed`), and an entry for a shift and date another entry already covers 409 (`availability.conflict`).
         */
        post: operations["add_entry_api_v1_employees__id__constraints_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/employees/{id}/deactivate": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Deactivate an employee
         * @description Manager or Admin. Block the user from signing in and end their sessions. Only an Admin may deactivate an Admin, and never the last active one (409 `users.last_admin`). An account that was invited but never accepted cannot be deactivated: it returns 409 (`users.pending_cannot_deactivate`) and stays pending.
         */
        post: operations["deactivate_employee_api_v1_employees__id__deactivate_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/employees/{id}/groups": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List an employee's groups
         * @description Manager or Admin for any user in the organisation; every other role only for their own id.
         */
        get: operations["get_employee_groups_api_v1_employees__id__groups_get"];
        /**
         * Replace an employee's groups
         * @description Manager or Admin. Set the user's groups to exactly the given ids (an empty list removes all). Every id must belong to the organisation, otherwise 404 and nothing changes.
         */
        put: operations["replace_employee_groups_api_v1_employees__id__groups_put"];
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/employees/{id}/qualifications": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List an employee's qualifications
         * @description Manager or Admin for any user in the organisation; every other role only for their own id.
         */
        get: operations["get_employee_qualifications_api_v1_employees__id__qualifications_get"];
        /**
         * Replace an employee's qualifications
         * @description Manager or Admin. Set the user's qualifications to exactly the given ids (an empty list removes all). Every id must belong to the organisation, otherwise 404 and nothing changes.
         */
        put: operations["replace_employee_qualifications_api_v1_employees__id__qualifications_put"];
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/employees/{id}/reactivate": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Reactivate an employee
         * @description Manager or Admin. Allow a deactivated user to sign in again. Only an Admin may reactivate an Admin. An account that was invited but never accepted cannot be reactivated: it returns 409 (`users.pending_cannot_reactivate`) and stays pending, because only accepting the invitation activates it. A portal user whose client is inactive or deleted returns 409 (`users.client_inactive`).
         */
        post: operations["reactivate_employee_api_v1_employees__id__reactivate_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/groups": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List groups
         * @description Manager or Admin. Return the organisation's groups ordered by name.
         */
        get: operations["list_groups_api_v1_groups_get"];
        put?: never;
        /**
         * Create a group
         * @description Manager or Admin. Create a group. Names are unique within the organisation (409 `groups.duplicate_name`).
         */
        post: operations["create_group_api_v1_groups_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/groups/{id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post?: never;
        /**
         * Delete a group
         * @description Manager or Admin. Delete the group and its memberships. The employees in it are kept.
         */
        delete: operations["delete_group_api_v1_groups__id__delete"];
        options?: never;
        head?: never;
        /**
         * Rename a group
         * @description Manager or Admin. Change a group's name. A name already used by another group returns 409 (`groups.duplicate_name`).
         */
        patch: operations["rename_group_api_v1_groups__id__patch"];
        trace?: never;
    };
    "/api/v1/organisation/contact": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Read the contact card
         * @description Every role, a client included. The organisation's phone, e-mail and opening hours, shown on the contact card and on every no-access page.
         */
        get: operations["read_contact_api_v1_organisation_contact_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        /**
         * Change the contact card
         * @description Admin. Change the phone, e-mail or opening hours; `null` clears a field. Opening hours are up to seven days, each weekday once, closing after opening (`organisation.opening_hours_invalid`, 422).
         */
        patch: operations["change_contact_api_v1_organisation_contact_patch"];
        trace?: never;
    };
    "/api/v1/organisation/pay-settings": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Read the premium defaults
         * @description Manager or Admin. The organisation's premium defaults, which every project follows unless it sets its own value: normal lengths, overtime rates, the weekly rest and its rate, and the overtime increases on a rest or special day.
         */
        get: operations["read_defaults_api_v1_organisation_pay_settings_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        /**
         * Change the premium defaults
         * @description Admin. Change any default; every project that has not ended and follows it uses the new value, for its earlier shifts too. Lengths are 1 to 1440 minutes, rates 100 to 300 percent and increases above 0 and at most 200 points; a weekly rest whose ends meet is refused (422 `pay.rest_window_invalid`).
         */
        patch: operations["change_defaults_api_v1_organisation_pay_settings_patch"];
        trace?: never;
    };
    "/api/v1/organisation/scheduling": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Read the scheduling limits
         * @description Manager or Admin. The minimum rest between shifts and the maximum hours per week, used by generation and by the warnings of manual rostering.
         */
        get: operations["read_settings_api_v1_organisation_scheduling_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        /**
         * Change the scheduling limits
         * @description Admin. Change the minimum rest (1 to 24 hours) or the weekly maximum (1 to 168 hours); later generation and warnings use the new values.
         */
        patch: operations["change_settings_api_v1_organisation_scheduling_patch"];
        trace?: never;
    };
    "/api/v1/projects": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List projects
         * @description Manager or Admin see every project of the organisation, optionally filtered by status or client. A Client sees only their own client's projects whatever filter is sent, and none if the account has no client. Employees receive 403.
         */
        get: operations["list_projects_api_v1_projects_get"];
        put?: never;
        /**
         * Create a project
         * @description Manager or Admin. Create a project for a client and a site of the organisation. The billing rate must be greater than zero (`projects.billing_rate_invalid`) and the end date must not be before the start date (`projects.date_range_invalid`). Only an Admin may send `pay_settings` (403 `projects.pay_settings_admin_only`).
         */
        post: operations["create_project_api_v1_projects_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/projects/{id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get a project
         * @description Manager or Admin for any project; a Client only for their own client's projects (otherwise 403 `projects.access_denied`), and 404 if the account has no client. Employees receive 403.
         */
        get: operations["get_project_api_v1_projects__id__get"];
        put?: never;
        post?: never;
        /**
         * Delete a project
         * @description Admin only. Soft-delete the project; the record is kept and hidden.
         */
        delete: operations["delete_project_api_v1_projects__id__delete"];
        options?: never;
        head?: never;
        /**
         * Edit a project
         * @description Manager or Admin. Update name, billing rate, end date or status. The same rate and date rules as creation apply. An explicit null clears a field that allows it and is refused with 422 on a required one; an omitted field is left alone.
         */
        patch: operations["edit_project_api_v1_projects__id__patch"];
        trace?: never;
    };
    "/api/v1/projects/{id}/copy-week": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Copy a week of the roster
         * @description A Manager of the project, or an Admin. Copy the week starting on Sunday `from_week` to the one starting on `to_week`, at the same local times, with the same people and their own times. Where a shift of the same type already starts at that time, `on_conflict` decides: `allow` adds it anyway, `overwrite` replaces a draft (never a published shift), `unfilled` adds it without people. `dry_run` answers the plan and changes nothing. A week that does not start on a Sunday is refused with 422 (`roster.week_not_sunday`).
         */
        post: operations["copy_week_api_v1_projects__id__copy_week_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/projects/{id}/managers": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List a project's managers
         * @description Manager or Admin. The Managers and Admins assigned to the project, by name.
         */
        get: operations["list_managers_api_v1_projects__id__managers_get"];
        /**
         * Set a project's managers
         * @description Admin. Replace the project's managers as a whole. Each must be an active Manager or Admin of the organisation (422 `project_managers.not_staff`). A Manager sees the project's pay only while assigned and the project's pay is open to Managers.
         */
        put: operations["replace_managers_api_v1_projects__id__managers_put"];
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/projects/{id}/pay-settings": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Read a project's pay settings
         * @description Manager or Admin. Whether the project has pay, whether its assigned Managers see it, the premium settings in effect and which of them are the project's own.
         */
        get: operations["read_project_pay_api_v1_projects__id__pay_settings_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        /**
         * Change a project's pay settings
         * @description Admin. Switch pay on or off, open it to the project's Managers, or give a setting the project's own value (`null` returns it to the default). An ended project's settings are fixed (409 `pay.project_ended`).
         */
        patch: operations["change_project_pay_api_v1_projects__id__pay_settings_patch"];
        trace?: never;
    };
    "/api/v1/projects/{id}/publish": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Publish drafts
         * @description Manager or Admin. Publish the project's draft shifts that start on `from` to `to` (dates at the project's site); published ones are left as they are. A range that ends before it starts is refused with 422 (`shifts.date_range_invalid`). One event per person on the published shifts is recorded for delivery, with `notify` (default true) as the publisher's choice.
         */
        post: operations["publish_api_v1_projects__id__publish_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/projects/{id}/publish-preview": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Review a publish before making it
         * @description Manager or Admin. What publishing the project's drafts that start on `from` to `to` (dates at the site) would make live: the shifts, the people on them, filled against required, unfilled seats, and warnings without an override.
         */
        get: operations["publish_preview_api_v1_projects__id__publish_preview_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/projects/{id}/requirements": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List a project's staffing needs
         * @description Manager or Admin. How many people the project needs per shift type and weekday.
         */
        get: operations["list_needs_api_v1_projects__id__requirements_get"];
        /**
         * Set a project's staffing needs
         * @description Manager or Admin. Replace the project's needs as a whole. Each shift type must be one the project uses (422 `requirements.shift_type_not_in_project`) and not retired (422 `requirements.shift_type_retired`); a shift type and weekday appear once (422 `requirements.duplicate`).
         */
        put: operations["replace_needs_api_v1_projects__id__requirements_put"];
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/projects/{id}/roster/bulk": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Change many drafts at once
         * @description A Manager of the project, or an Admin. Give every draft of the project that starts on `from` to `to` a new `headcount`, or `remove` them; published shifts are never touched. Exactly one of the two (422 `roster.bulk_change_invalid`). `dry_run` lists exactly the drafts it would hit and changes nothing.
         */
        post: operations["bulk_api_v1_projects__id__roster_bulk_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/projects/{id}/shift-types": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List a project's shift types
         * @description Manager or Admin. The shift types the project uses.
         */
        get: operations["project_types_api_v1_projects__id__shift_types_get"];
        /**
         * Set a project's shift types
         * @description Manager or Admin. Replace the set of shift types the project uses. Another organisation's type is 404; a retired one is refused with 422 (`shift_types.retired`). Removing a type leaves the shifts already made from it.
         */
        put: operations["set_project_types_api_v1_projects__id__shift_types_put"];
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/projects/{id}/special-days": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List the special days a project is paid by
         * @description Manager or Admin. The organisation's days and the project's own, by date; on a date both list, the project's wins. An ended project has only its own, copied when it ended.
         */
        get: operations["list_project_days_api_v1_projects__id__special_days_get"];
        put?: never;
        /**
         * Add a project's special day
         * @description Admin. A day for this project only, or this project's rate for an organisation day. One per project and date (409 `special_days.duplicate`); an ended project is fixed (409 `pay.project_ended`).
         */
        post: operations["add_project_day_api_v1_projects__id__special_days_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/projects/{id}/week-templates": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List the project's saved weeks
         * @description Manager or Admin. The project's week templates, by name.
         */
        get: operations["list_templates_api_v1_projects__id__week_templates_get"];
        put?: never;
        /**
         * Save a week as a template
         * @description A Manager of the project, or an Admin. Save the project's week starting on Sunday `week` under `name`, unique per project (409 `templates.duplicate_name`).
         */
        post: operations["save_template_api_v1_projects__id__week_templates_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/projects/{id}/week-templates/{template_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post?: never;
        /**
         * Remove a saved week
         * @description A Manager of the project, or an Admin. Remove one of the project's templates.
         */
        delete: operations["delete_template_api_v1_projects__id__week_templates__template_id__delete"];
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/projects/{id}/week-templates/{template_id}/apply": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Apply a saved week
         * @description A Manager of the project, or an Admin. Create the template's shifts in the week starting on Sunday `week`; `on_conflict` and `dry_run` work as for copy week.
         */
        post: operations["apply_template_api_v1_projects__id__week_templates__template_id__apply_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/qualifications": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List qualifications
         * @description Manager or Admin. Return the organisation's qualifications ordered by name.
         */
        get: operations["list_qualifications_api_v1_qualifications_get"];
        put?: never;
        /**
         * Create a qualification
         * @description Manager or Admin. Create a qualification. Names are unique within the organisation (409 `qualifications.duplicate_name`).
         */
        post: operations["create_qualification_api_v1_qualifications_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/qualifications/{id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post?: never;
        /**
         * Delete a qualification
         * @description Manager or Admin. Delete the qualification and its assignments. The employees who held it are kept.
         */
        delete: operations["delete_qualification_api_v1_qualifications__id__delete"];
        options?: never;
        head?: never;
        /**
         * Rename a qualification
         * @description Manager or Admin. Change a qualification's name. A name already used by another qualification returns 409 (`qualifications.duplicate_name`).
         */
        patch: operations["rename_qualification_api_v1_qualifications__id__patch"];
        trace?: never;
    };
    "/api/v1/reports/hours": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Hours and pay per employee or project
         * @description Manager or Admin. Hours and shift counts from `from` to `to` (at most 366 days, 422 `reports.range_invalid`), grouped by employee or by project, with the grand totals. An Admin sees pay for every project with pay on; a Manager only for projects they are assigned to whose pay is open to Managers. Elsewhere there is no pay figure at all, and pay totals cover only the lines shown with pay.
         */
        get: operations["hours_api_v1_reports_hours_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/reports/work-log": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Who worked at a project, site or group
         * @description Manager or Admin. Each person once per project with their shift count, hours and first and last date in the range, for exactly one of a project, a site or a group (422 `reports.scope_invalid`). For a project the range starts on its start date unless `from` is given, and then covers the whole project with no length limit; a site or a group needs `from` (422 `reports.start_required`). `to` is today by default. A range with `from` covers at most 366 days (422 `reports.range_invalid`). Only published shifts that have ended count; no pay is shown.
         */
        get: operations["work_log_api_v1_reports_work_log_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/schedule-jobs": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List schedule jobs
         * @description Manager or Admin. The organisation's jobs, newest first, without their proposals; `status` filters them. `limit` (1 to 200, default 50) per page; pass `next_cursor` back as `cursor` for the next.
         */
        get: operations["list_jobs_api_v1_schedule_jobs_get"];
        put?: never;
        /**
         * Ask for a roster
         * @description Manager or Admin. Queue a run that proposes a roster for `from` to `to` (at most 31 days) over every active project, or the `project_ids` given. A worker solves it; follow it with `GET /schedule-jobs/{id}`. Nothing in the roster changes until the proposal is accepted. A backwards or too long range is refused with 422 (`schedule.range_invalid`), a project that is not active with 422 (`schedule.project_not_active`). With an `Idempotency-Key`, a repeat of the same request answers the first job, and the same key with another request is refused with 422 (`idempotency.key_reused`).
         */
        post: operations["create_job_api_v1_schedule_jobs_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/schedule-jobs/{id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Read a schedule job
         * @description Manager or Admin. The job's status, range, projects, limits and times; once it has succeeded, whether its proposal is proven optimal, the proposal's state (open, accepted, discarded or expired after 7 days) and the proposal itself.
         */
        get: operations["read_job_api_v1_schedule_jobs__id__get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/schedule-jobs/{id}/accept": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Accept a proposal
         * @description Manager or Admin. Store the proposal as draft shifts and assignments, all or nothing; nothing is published. An existing draft keeps its id and headcount. Refused with 409 when the job has no proposal (`schedule.not_ready`), the proposal was accepted, discarded or has expired (`schedule.proposal_closed`), or anything it was made from has changed since (`schedule.proposal_out_of_date`); nothing changes then.
         */
        post: operations["accept_job_api_v1_schedule_jobs__id__accept_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/schedule-jobs/{id}/discard": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Discard a proposal
         * @description Manager or Admin. Close the proposal without storing it. Refused with 409 when the job has no proposal (`schedule.not_ready`) or it is already closed (`schedule.proposal_closed`).
         */
        post: operations["discard_job_api_v1_schedule_jobs__id__discard_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/shift-types": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List shift types
         * @description Employee, Manager or Admin. Every shift type of the organisation, active first; a retired one has `is_active` false.
         */
        get: operations["list_types_api_v1_shift_types_get"];
        put?: never;
        /**
         * Add a shift type
         * @description Manager or Admin. It crosses midnight when its end is not after its start. An active name already in use, in any letter case, is refused with 409 (`shift_types.duplicate_name`).
         */
        post: operations["add_type_api_v1_shift_types_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/shift-types/{id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        /**
         * Change or retire a shift type
         * @description Manager or Admin. Rename, retime, retire (`is_active` false) or bring back a type. Shifts already made from it keep their times. A name clash with an active type is refused with 409 (`shift_types.duplicate_name`).
         */
        patch: operations["change_type_api_v1_shift_types__id__patch"];
        trace?: never;
    };
    "/api/v1/shifts": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List the roster
         * @description Manager or Admin. Shifts that start on `from` to `to` (dates in the organisation's time zone), drafts and published, each with who is on it, ordered by start. `limit` (1 to 200, default 50) per page; pass `next_cursor` back as `cursor` for the next.
         */
        get: operations["list_shifts_api_v1_shifts_get"];
        put?: never;
        /**
         * Create a shift
         * @description A Manager of the project, or an Admin; only an Admin sets `pay_rate` (403 `shift.pay_rate_admin_only`), and a Manager of other projects is refused with 403 (`roster.project_not_managed`). A draft shift of one of the project's shift types on `date`, at the type's local times at the project's site unless `starts_at` and `ends_at` are both given. A type the project does not use is refused with 422 (`shifts.type_not_in_project`), times that do not end after they start with 422 (`shifts.times_invalid`). The answer lists the warnings the shift causes.
         */
        post: operations["create_shift_api_v1_shifts_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/shifts/{id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post?: never;
        /**
         * Delete a shift
         * @description A Manager of the project, or an Admin. Delete the shift and its assignments; a stale `If-Match` is refused with 412 (`roster.changed`).
         */
        delete: operations["delete_shift_api_v1_shifts__id__delete"];
        options?: never;
        head?: never;
        /**
         * Change a shift
         * @description A Manager of the project, or an Admin; only an Admin changes `pay_rate`. Send the shift's `etag` as `If-Match` to be refused with 412 (`roster.changed`) if it changed since. Change the times, headcount or pay rate; a published shift stays published. Times that do not end after they start are refused with 422 (`shifts.times_invalid`). The answer lists the warnings the change causes.
         */
        patch: operations["change_shift_api_v1_shifts__id__patch"];
        trace?: never;
    };
    "/api/v1/shifts/{id}/assignments": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Assign an employee
         * @description A Manager of the project, or an Admin. `starts_at` and `ends_at`, both or neither, are the person's own times, at most 4 hours before the shift starts and after it ends (422 `assignment.times_out_of_range`). A stale `If-Match` is refused with 412. Put an active employee on the shift; the answer lists the warnings it causes, and none refuses it. An inactive employee is refused with 409 (`assignments.employee_inactive`), the same employee twice with 409 (`assignments.duplicate`). With an `Idempotency-Key`, a repeat of the same request in the next 24 hours gets the first answer and changes nothing; the same key with another request is refused with 422 (`idempotency.key_reused`).
         */
        post: operations["assign_api_v1_shifts__id__assignments_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/sites": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List sites
         * @description Manager or Admin. Return the organisation's sites ordered by name.
         */
        get: operations["list_sites_api_v1_sites_get"];
        put?: never;
        /**
         * Create a site
         * @description Manager or Admin. Create a site with its address and IANA time zone. A time zone that is not an exact IANA name is refused with 422 (`sites.timezone_invalid`). Sites cannot be deleted.
         */
        post: operations["create_site_api_v1_sites_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/sites/{id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get a site
         * @description Manager or Admin. Return one site of the organisation.
         */
        get: operations["get_site_api_v1_sites__id__get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        /**
         * Edit a site
         * @description Manager or Admin. Update a site's name, address or time zone. A time zone that is not an exact IANA name is refused with 422 (`sites.timezone_invalid`). Every field here is required, so null is refused with 422; an omitted field is left alone.
         */
        patch: operations["edit_site_api_v1_sites__id__patch"];
        trace?: never;
    };
    "/api/v1/special-days": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List the organisation's special days
         * @description Manager or Admin. The organisation's special days by date, of one year if given.
         */
        get: operations["list_days_api_v1_special_days_get"];
        put?: never;
        /**
         * Add a special day
         * @description Admin. A date paid at its own rate (100 to 300 percent, 150.00 by default) for every project that has not ended. One day per date (409 `special_days.duplicate`).
         */
        post: operations["add_day_api_v1_special_days_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/special-days/{id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post?: never;
        /**
         * Delete a special day
         * @description Admin. Delete an organisation or project day; a project's own day on the same date stays. A day of an ended project is fixed (409 `pay.project_ended`).
         */
        delete: operations["delete_day_api_v1_special_days__id__delete"];
        options?: never;
        head?: never;
        /**
         * Change a special day
         * @description Admin. Change an organisation or project day's date, name or rate. A day of an ended project is fixed (409 `pay.project_ended`); a date already taken is refused (409 `special_days.duplicate`).
         */
        patch: operations["change_day_api_v1_special_days__id__patch"];
        trace?: never;
    };
    "/auth/invite": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Invite a user
         * @description Manager or Admin. Invite an email address and email a one-time invitation token valid for 7 days. A new address gets a pending account (never activated). Re-inviting a pending account of the same organisation sets the new role and replaces the earlier link, unless it is a Client account, whose role cannot change (409 `users.client_role_locked`). Only an Admin may invite an Admin or change a pending Admin; Client users are created from the client record (`users.role_invalid`). An email that belongs to an active account, or to another organisation, returns 409 (`users.duplicate_email`); a deactivated account returns 409 (`invitations.account_deactivated`) and must be reactivated by staff instead. Answers 503 (`service.busy`) with `Retry-After` when the password-hashing queue is full; nothing is changed.
         */
        post: operations["create_invitation_auth_invite_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/auth/invite/accept": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Accept an invitation
         * @description Public. Redeem an invitation token to set the password and activate a pending account. A token for an account that is not pending (already active, or deactivated) returns 401 (`auth.token_used`) and changes nothing, as does a spent or cancelled token; an unknown or expired token returns 401 (`auth.token_expired`). The password needs at least 12 characters. Answers 503 (`service.busy`) with `Retry-After` when the password-hashing queue is full; nothing is changed.
         */
        post: operations["accept_invitation_auth_invite_accept_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/auth/login": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Sign in
         * @description Exchange an email and password for an access token and a refresh token. The email is matched without regard to letter case. Unknown email, wrong password and inactive account all return the same 401 (`auth.invalid_credentials`), after the same amount of work. Any address, registered or not, locks after exactly the configured number of consecutive failures (default 5) and the call then returns 423 (`auth.account_locked`) until the cooldown ends, even with the right password; the count restarts at zero when it does, and a successful sign-in also clears it. Too many failures from one network address inside the window return 429 (`auth.rate_limited`) with `Retry-After`, for every address; a successful sign-in does not clear that count. Answers 503 (`service.busy`) with `Retry-After` when the password-hashing queue is full; nothing is changed. `client_kind` `native` (the phone app) gets a refresh token that lasts `REFRESH_TOKEN_TTL_DAYS_NATIVE` days (30) and renews on every refresh; `web`, the default, keeps `REFRESH_TOKEN_TTL_DAYS` (7).
         */
        post: operations["login_auth_login_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/auth/logout": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Sign out
         * @description Revoke one of the caller's refresh tokens, ending that session. Requires a valid access token. An unknown or already revoked token, or one of another user, changes nothing and still returns 204.
         */
        post: operations["logout_auth_logout_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/auth/logout-all": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Sign out everywhere
         * @description Revoke every refresh token of the signed-in user. Access tokens already issued remain valid until they expire.
         */
        post: operations["logout_all_auth_logout_all_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/auth/password-reset/confirm": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Confirm a password reset
         * @description Public. Redeem a reset token to set a new password for an active account. All of the user's refresh tokens are revoked, ending every session, every other outstanding token of the account is cancelled, and any sign-in lock is cleared. An unknown or expired token returns 401 (`auth.token_expired`); a spent or cancelled token, or a token for an account that is not active, returns 401 (`auth.token_used`) and changes nothing. Answers 503 (`service.busy`) with `Retry-After` when the password-hashing queue is full; nothing is changed.
         */
        post: operations["confirm_password_reset_auth_password_reset_confirm_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/auth/password-reset/request": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Request a password reset
         * @description Public. Email a one-time reset token valid for 60 minutes if the address belongs to an active account (letter case is ignored). The request only counts and answers: the account is looked at and the email sent after the response, so a failure there is not reported, and an email can be lost if the service stops. Earlier tokens keep working until one is redeemed, which cancels the rest. Requests are limited per address and per network in a 60-minute window; over a limit nothing is issued or sent. Always answers 202, whether or not the address is registered and whether or not a limit is reached, so it cannot be used to find out who has an account.
         */
        post: operations["request_password_reset_auth_password_reset_request_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/auth/refresh": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Refresh tokens
         * @description Exchange a refresh token for a new access and refresh token. The presented refresh token is spent and works exactly once, even if sent twice at the same moment: using it again returns 401 (`auth.token_revoked`). If it was replaced more than the grace window ago (default 10 seconds), every session of the user also ends, because it may be a stolen copy. An expired token returns `auth.token_expired` and ends no session, even if it was already used.
         */
        post: operations["refresh_token_auth_refresh_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/me": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get my profile
         * @description Return the signed-in user's own profile, including their default hourly rate (view only), the capabilities they hold now and, for a Manager, the projects they manage. The client draws its navigation from `capabilities` only. Available to every role.
         */
        get: operations["get_me_me_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        /**
         * Update my profile
         * @description Change the signed-in user's own full name or phone. Any other field is refused with 403 (`profile.field_not_allowed`), so role, rate, email and activity cannot be changed here. An explicit null clears a field that allows it and is refused with 422 on a required one; an omitted field is left alone.
         */
        patch: operations["patch_me_me_patch"];
        trace?: never;
    };
    "/me/availability-submissions": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Read my availability status for a window
         * @description Employee, Manager or Admin. Where the caller's own availability for `window_id` stands: not submitted, draft, submitted (with the time) or locked after the deadline, and whether a missed deadline counts them as unavailable for the whole window.
         */
        get: operations["read_submission_me_availability_submissions_get"];
        put?: never;
        /**
         * Submit my availability for a window
         * @description Employee, Manager or Admin. Submit the caller's blocked, open and close entries for an open window; submitting again replaces the earlier submission. Answers 409 (`availability.window_closed`) once the deadline has passed. After the deadline, scheduling uses what was submitted; with nothing submitted the caller counts as unavailable for the whole window.
         */
        post: operations["submit_me_availability_submissions_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/me/pay": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Read my pay so far this month
         * @description Employee, Manager or Admin. The caller's gross pay, before tax, for a month (`YYYY-MM`, the current one by default): each published shift that has ended, with its hours at each rate and its pay, and the totals. Draft shifts and shifts not yet ended are not counted. A shift belongs to the month it starts in at its site. 422 `pay.month_invalid`.
         */
        get: operations["my_pay_me_pay_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/me/sessions": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List my sessions
         * @description The signed-in user's live sessions, one per device, newest first. Available to every role. `POST /auth/logout-all` ends them all.
         */
        get: operations["list_sessions_me_sessions_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/me/sessions/{id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post?: never;
        /**
         * End one of my sessions
         * @description Sign one device out by revoking its refresh token; its access token lapses within 15 minutes. Answers 404 (`resource.not_found`) for an id that is not one of the caller's live sessions. Available to every role.
         */
        delete: operations["end_session_me_sessions__id__delete"];
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/me/shifts": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List my shifts
         * @description Employee, Manager or Admin. The caller's own published shifts that start on `from` to `to` (optional dates in the organisation's time zone), with the project, the site and its time zone. Never a draft, never anyone else's, and nothing about who else is on them.
         */
        get: operations["my_shifts_me_shifts_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/me/shifts/{id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Read one of my shifts
         * @description Employee, Manager or Admin. One of the caller's own published shifts with the site's address and the project's managers to contact, who stand in for a shift leader. Answers 404 for any shift the caller is not on, a draft included, and says nothing about who else is on it.
         */
        get: operations["my_shift_me_shifts__id__get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
}
export type webhooks = Record<string, never>;
export interface components {
    schemas: {
        /**
         * AcceptAnswer
         * @description What accepting wrote to the roster, as drafts.
         */
        AcceptAnswer: {
            /** Assignments Added */
            assignments_added: number;
            /** Assignments Removed */
            assignments_removed: number;
            /** Shifts Created */
            shifts_created: number;
            /** Shifts Updated */
            shifts_updated: number;
        };
        /**
         * AcceptInviteRequest
         * @description Invitation token and the chosen password (at least 12 characters).
         */
        AcceptInviteRequest: {
            /** Password */
            password: string;
            /** Token */
            token: string;
        };
        /**
         * AssignRequest
         * @description The employee to put on the shift, and optionally their own times in it (007 FR-027).
         */
        AssignRequest: {
            /** Ends At */
            ends_at?: string | null;
            /** Starts At */
            starts_at?: string | null;
            /**
             * User Id
             * Format: uuid
             */
            user_id: string;
        };
        /**
         * AssignmentAnswer
         * @description A stored assignment, the warnings it caused, and its shift's new ``etag``.
         */
        AssignmentAnswer: {
            assignment: components["schemas"]["AssignmentRead"];
            /** Shift Etag */
            shift_etag: string;
            /** Warnings */
            warnings: components["schemas"]["WarningRead"][];
        };
        /**
         * AssignmentRead
         * @description An employee on a shift, and when they work it.
         *
         *     ``starts_at`` and ``ends_at`` are when this person works the shift: their own times when set,
         *     otherwise the shift's; ``own_times`` says which (007 FR-026, FR-027).
         */
        AssignmentRead: {
            /**
             * Ends At
             * Format: date-time
             */
            ends_at: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /**
             * Overrides
             * @default []
             */
            overrides: components["schemas"]["OverrideRead"][];
            /** Own Times */
            own_times: boolean;
            /**
             * Shift Id
             * Format: uuid
             */
            shift_id: string;
            /**
             * Starts At
             * Format: date-time
             */
            starts_at: string;
            /**
             * User Id
             * Format: uuid
             */
            user_id: string;
        };
        /**
         * AssignmentTimesRequest
         * @description A person's own times in a shift; both null gives them the shift's times again.
         */
        AssignmentTimesRequest: {
            /** Ends At */
            ends_at: string | null;
            /** Starts At */
            starts_at: string | null;
        };
        /**
         * BulkRequest
         * @description Change every draft of the project on ``from`` to ``to``: a new headcount, or remove.
         */
        BulkRequest: {
            /**
             * Dry Run
             * @default false
             */
            dry_run: boolean;
            /**
             * From
             * Format: date
             */
            from: string;
            /** Headcount */
            headcount?: number | null;
            /**
             * Remove
             * @default false
             */
            remove: boolean;
            /**
             * To
             * Format: date
             */
            to: string;
        };
        /**
         * Change
         * @description A person on a slot, added or removed against the current drafts.
         */
        Change: {
            /** Key */
            key: string;
            /**
             * User Id
             * Format: uuid
             */
            user_id: string;
        };
        /**
         * Changes
         * @description The proposal against the current drafts.
         */
        Changes: {
            /** Added */
            added: components["schemas"]["Change"][];
            /** Removed */
            removed: components["schemas"]["Change"][];
        };
        /**
         * ClientCreateRequest
         * @description New client; ``contract_start`` is required and dates are ISO-8601 strings.
         */
        ClientCreateRequest: {
            /** Contact Email */
            contact_email?: string | null;
            /** Contact Name */
            contact_name?: string | null;
            /** Contact Phone */
            contact_phone?: string | null;
            /** Contract End */
            contract_end?: string | null;
            /**
             * Contract Start
             * Format: date
             */
            contract_start: string;
            /** Name */
            name: string;
        };
        /**
         * ClientListResponse
         * @description The organisation's clients.
         */
        ClientListResponse: {
            /** Items */
            items: components["schemas"]["ClientRead"][];
        };
        /**
         * ClientRead
         * @description A client and its contract dates.
         */
        ClientRead: {
            /** Contact Email */
            contact_email?: string | null;
            /** Contact Name */
            contact_name?: string | null;
            /** Contact Phone */
            contact_phone?: string | null;
            /** Contract End */
            contract_end?: string | null;
            /**
             * Contract Start
             * Format: date
             */
            contract_start: string;
            /**
             * Created At
             * Format: date-time
             */
            created_at: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Is Active */
            is_active: boolean;
            /** Name */
            name: string;
            /**
             * Organization Id
             * Format: uuid
             */
            organization_id: string;
            /**
             * Updated At
             * Format: date-time
             */
            updated_at: string;
        };
        /**
         * ClientUpdateRequest
         * @description Client fields that may change; omitted fields are left as they are, null clears.
         */
        ClientUpdateRequest: {
            /** Contact Email */
            contact_email?: string | null;
            /** Contact Name */
            contact_name?: string | null;
            /** Contact Phone */
            contact_phone?: string | null;
            /** Contract End */
            contract_end?: string | null;
            /**
             * Contract Start
             * Format: date
             */
            contract_start?: string;
            /** Is Active */
            is_active?: boolean;
            /** Name */
            name?: string;
        };
        /**
         * ClientUserCreateRequest
         * @description New portal account; the initial password needs at least 12 characters.
         */
        ClientUserCreateRequest: {
            /**
             * Email
             * Format: email
             */
            email: string;
            /** Full Name */
            full_name: string;
            /** Initial Password */
            initial_password: string;
            /** Phone */
            phone?: string | null;
        };
        /**
         * ClientUserRead
         * @description A client portal account.
         */
        ClientUserRead: {
            /** Client Id */
            client_id?: string | null;
            /**
             * Created At
             * Format: date-time
             */
            created_at: string;
            /** Email */
            email: string;
            /** Full Name */
            full_name: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Is Active */
            is_active: boolean;
            /**
             * Organization Id
             * Format: uuid
             */
            organization_id: string;
            /** Phone */
            phone?: string | null;
            /**
             * Updated At
             * Format: date-time
             */
            updated_at: string;
        };
        /**
         * CopyWeekRequest
         * @description Copy one week of the project's roster to another; both weeks start on a Sunday.
         */
        CopyWeekRequest: {
            /**
             * Dry Run
             * @default false
             */
            dry_run: boolean;
            /**
             * From Week
             * Format: date
             */
            from_week: string;
            /**
             * On Conflict
             * @default allow
             * @enum {string}
             */
            on_conflict: "allow" | "overwrite" | "unfilled";
            /**
             * To Week
             * Format: date
             */
            to_week: string;
        };
        /**
         * DayInEffectRead
         * @description A day as a project's shifts are paid by it: the project's own, or the organisation's.
         */
        DayInEffectRead: {
            /**
             * Day
             * Format: date
             */
            day: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Name */
            name: string;
            /** Project Id */
            project_id: string | null;
            /**
             * Rate Pct
             * @example 125.00
             */
            rate_pct: string;
            /**
             * Source
             * @description `project` or `organisation`.
             */
            source: string;
        };
        /**
         * DaysInEffect
         * @description The days a project's shifts are paid by, by date.
         */
        DaysInEffect: {
            /** Items */
            items: components["schemas"]["DayInEffectRead"][];
        };
        /**
         * EmployeeShare
         * @description A candidate's share of the range.
         */
        EmployeeShare: {
            /** Close Not Met */
            close_not_met: number;
            /** Minutes */
            minutes: number;
            /** Nights */
            nights: number;
            /** Open Met */
            open_met: number;
            /**
             * User Id
             * Format: uuid
             */
            user_id: string;
        };
        /**
         * EntryCreateRequest
         * @description A new entry over ``starts_on`` to ``ends_on`` (inclusive).
         *
         *     ``absence_kind`` is set for an absence and only then. Blocked, open and close entries may be
         *     limited to one ``shift_type_id`` and one ``weekday``; an absence takes neither.
         */
        EntryCreateRequest: {
            /** Absence Kind */
            absence_kind?: ("leave" | "reserve_duty" | "sick") | null;
            /**
             * Ends On
             * Format: date
             */
            ends_on: string;
            /**
             * Kind
             * @enum {string}
             */
            kind: "blocked" | "open" | "close" | "absence";
            /** Shift Type Id */
            shift_type_id?: string | null;
            /**
             * Starts On
             * Format: date
             */
            starts_on: string;
            /**
             * Weekday
             * @description 0 Sunday to 6 Saturday.
             */
            weekday?: number | null;
        };
        /**
         * EntryListResponse
         * @description Entries in date order.
         */
        EntryListResponse: {
            /** Items */
            items: components["schemas"]["EntryRead"][];
        };
        /**
         * EntryRead
         * @description An availability entry.
         */
        EntryRead: {
            /** Absence Kind */
            absence_kind: ("leave" | "reserve_duty" | "sick") | null;
            /**
             * Created At
             * Format: date-time
             */
            created_at: string;
            /**
             * Ends On
             * Format: date
             */
            ends_on: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /**
             * Kind
             * @enum {string}
             */
            kind: "blocked" | "open" | "close" | "absence";
            /** Shift Type Id */
            shift_type_id: string | null;
            /**
             * Starts On
             * Format: date
             */
            starts_on: string;
            /**
             * Updated At
             * Format: date-time
             */
            updated_at: string;
            /**
             * User Id
             * Format: uuid
             */
            user_id: string;
            /** Weekday */
            weekday: number | null;
        };
        /**
         * EntryUpdateRequest
         * @description Entry fields that may change; null clears an optional one, an omitted field stays.
         */
        EntryUpdateRequest: {
            /** Absence Kind */
            absence_kind?: ("leave" | "reserve_duty" | "sick") | null;
            /**
             * Ends On
             * Format: date
             */
            ends_on?: string;
            /**
             * Kind
             * @enum {string}
             */
            kind?: "blocked" | "open" | "close" | "absence";
            /** Shift Type Id */
            shift_type_id?: string | null;
            /**
             * Starts On
             * Format: date
             */
            starts_on?: string;
            /**
             * Weekday
             * @description 0 Sunday to 6 Saturday.
             */
            weekday?: number | null;
        };
        /**
         * GroupListResponse
         * @description The organisation's groups.
         */
        GroupListResponse: {
            /** Items */
            items: components["schemas"]["hds__routers__groups__GroupRead"][];
        };
        /**
         * GroupRenameRequest
         * @description The new name; unique within the organisation. Any other field is refused with 422.
         */
        GroupRenameRequest: {
            /** Name */
            name: string;
        };
        /**
         * GroupWriteRequest
         * @description Group name for create; unique within the organisation.
         */
        GroupWriteRequest: {
            /** Name */
            name: string;
        };
        /**
         * HoursGroupRead
         * @description One employee (rows per project) or one project (rows per employee).
         */
        HoursGroupRead: {
            /**
             * Hours
             * @example 7.50
             */
            hours: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Name */
            name: string;
            /**
             * Pay
             * @description Left out where pay is not shown, and where every line shown with pay lacks a rate (then shifts_missing_rate is above 0): a missing rate is never shown as zero.
             */
            pay?: string | null;
            /** Rows */
            rows: components["schemas"]["HoursRowRead"][];
            /** Shifts */
            shifts: number;
            /**
             * Shifts Missing Rate
             * @description Shifts shown with pay that have a line missing a rate; 0 where pay is hidden.
             */
            shifts_missing_rate: number;
        };
        /**
         * HoursReportRead
         * @description Hours and shifts by employee or project; pay totals cover only the lines shown with pay.
         */
        HoursReportRead: {
            /**
             * From
             * Format: date
             */
            from: string;
            /**
             * Group By
             * @enum {string}
             */
            group_by: "employee" | "project";
            /** Groups */
            groups: components["schemas"]["HoursGroupRead"][];
            /**
             * Hours
             * @example 7.50
             */
            hours: string;
            /**
             * Pay
             * @description Left out when no pay is shown or no line shown has a rate.
             */
            pay?: string | null;
            /** Shifts */
            shifts: number;
            /** Shifts Missing Rate */
            shifts_missing_rate: number;
            /**
             * To
             * Format: date
             */
            to: string;
        };
        /**
         * HoursRowRead
         * @description One employee at one project; ``pay`` only where the reader may see it.
         */
        HoursRowRead: {
            /**
             * Hours
             * @example 7.50
             */
            hours: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Name */
            name: string;
            /**
             * Pay
             * @description Left out where pay is not shown, and where every line shown with pay lacks a rate (then shifts_missing_rate is above 0): a missing rate is never shown as zero.
             */
            pay?: string | null;
            /** Shifts */
            shifts: number;
            /**
             * Shifts Missing Rate
             * @description Shifts shown with pay that have a line missing a rate; 0 where pay is hidden.
             */
            shifts_missing_rate: number;
        };
        /**
         * IDListRequest
         * @description The complete new set of ids; an empty list clears the set.
         */
        IDListRequest: {
            /** Ids */
            ids: string[];
        };
        /**
         * InviteRequest
         * @description Email and role to invite; the role defaults to employee.
         */
        InviteRequest: {
            /**
             * Email
             * Format: email
             */
            email: string;
            /** @default employee */
            role: components["schemas"]["UserRole"];
        };
        /**
         * JobCreateRequest
         * @description What to generate: a range of dates, and optionally some projects and the solver.
         */
        JobCreateRequest: {
            /**
             * From
             * Format: date
             */
            from: string;
            /** Project Ids */
            project_ids?: string[];
            /**
             * Solver
             * @default cpsat
             * @enum {string}
             */
            solver: "greedy" | "cpsat";
            /**
             * To
             * Format: date
             */
            to: string;
        };
        /**
         * JobPage
         * @description A page of jobs, newest first.
         */
        JobPage: {
            /** Items */
            items: components["schemas"]["JobSummary"][];
            /** Next Cursor */
            next_cursor: string | null;
        };
        /**
         * JobRead
         * @description A job and, once it has succeeded, its proposal.
         */
        JobRead: {
            /**
             * Created At
             * Format: date-time
             */
            created_at: string;
            /** Error Code */
            error_code: string | null;
            /** Finished At */
            finished_at: string | null;
            /**
             * From
             * Format: date
             */
            from: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Optimal */
            optimal: boolean | null;
            /** Project Ids */
            project_ids: string[];
            proposal: components["schemas"]["Proposal"] | null;
            /** Proposal State */
            proposal_state: ("open" | "accepted" | "discarded" | "expired") | null;
            /**
             * Requested By
             * Format: uuid
             */
            requested_by: string;
            settings: components["schemas"]["Limits"];
            /** Solver */
            solver: string;
            /** Started At */
            started_at: string | null;
            /**
             * Status
             * @enum {string}
             */
            status: "queued" | "running" | "succeeded" | "failed";
            /**
             * Time Limit Seconds
             * @description How long the solver may search: the determinate progress bar's whole (007 FR-074). Elapsed is now minus `started_at`.
             */
            time_limit_seconds: number;
            /**
             * To
             * Format: date
             */
            to: string;
        };
        /**
         * JobSummary
         * @description A job without its proposal.
         */
        JobSummary: {
            /**
             * Created At
             * Format: date-time
             */
            created_at: string;
            /** Error Code */
            error_code: string | null;
            /** Finished At */
            finished_at: string | null;
            /**
             * From
             * Format: date
             */
            from: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Optimal */
            optimal: boolean | null;
            /** Project Ids */
            project_ids: string[];
            /** Proposal State */
            proposal_state: ("open" | "accepted" | "discarded" | "expired") | null;
            /**
             * Requested By
             * Format: uuid
             */
            requested_by: string;
            settings: components["schemas"]["Limits"];
            /** Solver */
            solver: string;
            /** Started At */
            started_at: string | null;
            /**
             * Status
             * @enum {string}
             */
            status: "queued" | "running" | "succeeded" | "failed";
            /**
             * Time Limit Seconds
             * @description How long the solver may search: the determinate progress bar's whole (007 FR-074). Elapsed is now minus `started_at`.
             */
            time_limit_seconds: number;
            /**
             * To
             * Format: date
             */
            to: string;
        };
        /**
         * Limits
         * @description The limits a job ran with.
         */
        Limits: {
            /** Max Weekly Hours */
            max_weekly_hours: number;
            /** Min Rest Hours */
            min_rest_hours: number;
        };
        /**
         * LoginRequest
         * @description Credentials for ``POST /auth/login``.
         *
         *     ``client_kind`` says what is signing in: ``web`` (the default) or ``native`` (the phone
         *     app), whose refresh token lasts longer (007 FR-010, ADR 016).
         */
        LoginRequest: {
            /**
             * Client Kind
             * @default web
             * @enum {string}
             */
            client_kind: "web" | "native";
            /**
             * Email
             * Format: email
             */
            email: string;
            /** Password */
            password: string;
        };
        /**
         * ManagerRead
         * @description A Manager or Admin on the project.
         */
        ManagerRead: {
            /** Full Name */
            full_name: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
        };
        /**
         * ManagersRequest
         * @description The complete set of a project's managers; an empty list clears it.
         */
        ManagersRequest: {
            /** User Ids */
            user_ids: string[];
        };
        /**
         * ManagersResponse
         * @description A project's managers, by name.
         */
        ManagersResponse: {
            /** Items */
            items: components["schemas"]["ManagerRead"][];
        };
        /**
         * MeRead
         * @description The signed-in user's profile, with what they may do.
         *
         *     ``capabilities`` are the names in ``hds.core.capabilities``, sorted. ``managed_project_ids``
         *     are the projects a Manager manages, where their ``:project`` capabilities apply; it is empty
         *     for every other role (an Admin's scoped capabilities cover every project).
         */
        MeRead: {
            /** Capabilities */
            capabilities: string[];
            /**
             * Created At
             * Format: date-time
             */
            created_at: string;
            /** Default Hourly Rate */
            default_hourly_rate?: string | null;
            /**
             * Email
             * Format: email
             */
            email: string;
            /** Full Name */
            full_name: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Is Active */
            is_active: boolean;
            /** Managed Project Ids */
            managed_project_ids: string[];
            /**
             * Organization Id
             * Format: uuid
             */
            organization_id: string;
            /** Phone */
            phone?: string | null;
            role: components["schemas"]["UserRole"];
            /**
             * Updated At
             * Format: date-time
             */
            updated_at: string;
        };
        /**
         * MyShift
         * @description One of the caller's published shifts: UTC instants and the site's IANA time zone.
         */
        MyShift: {
            /**
             * Ends At
             * Format: date-time
             */
            ends_at: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            project: components["schemas"]["NamedRef"];
            /** Shift Type */
            shift_type: string;
            site: components["schemas"]["NamedRef"];
            /**
             * Starts At
             * Format: date-time
             */
            starts_at: string;
            /** Timezone */
            timezone: string;
        };
        /**
         * MyShiftDetail
         * @description One of the caller's published shifts, with the site's address and who to contact.
         */
        MyShiftDetail: {
            /** Contacts */
            contacts: components["schemas"]["ShiftContact"][];
            /**
             * Ends At
             * Format: date-time
             */
            ends_at: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            project: components["schemas"]["NamedRef"];
            /** Shift Type */
            shift_type: string;
            site: components["schemas"]["NamedRef"];
            /** Site Address */
            site_address: string | null;
            /**
             * Starts At
             * Format: date-time
             */
            starts_at: string;
            /** Timezone */
            timezone: string;
        };
        /**
         * MyShiftList
         * @description The caller's published shifts, by start time.
         */
        MyShiftList: {
            /** Items */
            items: components["schemas"]["MyShift"][];
        };
        /**
         * NamedRef
         * @description A record's id and name.
         */
        NamedRef: {
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Name */
            name: string;
        };
        /**
         * NeedItem
         * @description People needed on one shift type on one weekday; any other field is refused with 422.
         */
        NeedItem: {
            /** Headcount */
            headcount: number;
            /**
             * Shift Type Id
             * Format: uuid
             */
            shift_type_id: string;
            /**
             * Weekday
             * @description 0 Sunday to 6 Saturday.
             */
            weekday: number;
        };
        /**
         * NeedsRequest
         * @description A project's complete set of needs; an empty list removes them all.
         */
        NeedsRequest: {
            /** Items */
            items: components["schemas"]["NeedItem"][];
        };
        /**
         * NeedsResponse
         * @description A project's needs, by weekday.
         */
        NeedsResponse: {
            /** Items */
            items: components["schemas"]["NeedItem"][];
        };
        /**
         * OpeningHours
         * @description One day's opening hours in the organisation's local time (0 is Sunday).
         */
        OpeningHours: {
            /** Closes */
            closes: string;
            /** Opens */
            opens: string;
            /** Weekday */
            weekday: number;
        };
        /**
         * OrganisationContact
         * @description The contact card: phone, e-mail and opening hours, each optional.
         */
        OrganisationContact: {
            /** Email */
            email?: string | null;
            /**
             * Opening Hours
             * @default []
             */
            opening_hours: components["schemas"]["OpeningHours"][];
            /** Phone */
            phone?: string | null;
        };
        /**
         * OrganisationContactUpdate
         * @description Any of the card's fields; ``null`` clears one, an omitted one stays.
         */
        OrganisationContactUpdate: {
            /** Email */
            email?: string | null;
            /** Opening Hours */
            opening_hours?: components["schemas"]["OpeningHours"][] | null;
            /** Phone */
            phone?: string | null;
        };
        /**
         * OverrideCreated
         * @description A recorded override.
         */
        OverrideCreated: {
            /**
             * Assignment Id
             * Format: uuid
             */
            assignment_id: string;
            /**
             * Created At
             * Format: date-time
             */
            created_at: string;
            /**
             * Created By
             * Format: uuid
             */
            created_by: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Reason */
            reason: string;
            /** Warning Code */
            warning_code: string;
        };
        /**
         * OverrideRead
         * @description A recorded reason for keeping an assignment despite a warning (007 FR-034).
         */
        OverrideRead: {
            /**
             * Created At
             * Format: date-time
             */
            created_at: string;
            /**
             * Created By
             * Format: uuid
             */
            created_by: string;
            /** Reason */
            reason: string;
            /** Warning Code */
            warning_code: string;
        };
        /**
         * OverrideRequest
         * @description The warning to keep the assignment despite, and why.
         */
        OverrideRequest: {
            /** Reason */
            reason: string;
            /** Warning Code */
            warning_code: string;
        };
        /**
         * PartRead
         * @description Hours paid at one rate, in percent of the base rate.
         */
        PartRead: {
            /**
             * Hours
             * @example 7.50
             */
            hours: string;
            /**
             * Rate Pct
             * @example 125.00
             */
            rate_pct: string;
        };
        /**
         * PasswordResetConfirm
         * @description Reset token and the new password (at least 12 characters).
         */
        PasswordResetConfirm: {
            /** New Password */
            new_password: string;
            /** Token */
            token: string;
        };
        /**
         * PasswordResetRequest
         * @description Email address that asked for a password reset.
         */
        PasswordResetRequest: {
            /**
             * Email
             * Format: email
             */
            email: string;
        };
        /**
         * PayLineRead
         * @description One ended shift of the caller's, priced.
         *
         *     ``pay`` is left out when the project has pay switched off (``pay_enabled`` false) or when there
         *     is no rate (``rate_missing``): missing pay is never shown as zero.
         */
        PayLineRead: {
            /**
             * Base Rate
             * @description The hourly rate used, a decimal string.
             */
            base_rate: string | null;
            /**
             * Date
             * Format: date
             */
            date: string;
            /**
             * Ends At
             * Format: date-time
             */
            ends_at: string;
            /**
             * Hours
             * @example 7.50
             */
            hours: string;
            /** Parts */
            parts: components["schemas"]["PartRead"][];
            /**
             * Pay
             * @description Left out when there is no pay.
             */
            pay?: string | null;
            /** Pay Enabled */
            pay_enabled: boolean;
            /**
             * Project Id
             * Format: uuid
             */
            project_id: string;
            /** Project Name */
            project_name: string;
            /** Rate Missing */
            rate_missing: boolean;
            /**
             * Shift Id
             * Format: uuid
             */
            shift_id: string;
            /**
             * Starts At
             * Format: date-time
             */
            starts_at: string;
            /**
             * Timezone
             * @description The site's IANA time zone, to show the times as there.
             */
            timezone: string;
        };
        /**
         * PayToDateRead
         * @description The caller's pay for a month so far: each ended shift, and the totals of the lines shown.
         */
        PayToDateRead: {
            /**
             * Hours
             * @example 7.50
             */
            hours: string;
            /** Lines */
            lines: components["schemas"]["PayLineRead"][];
            /**
             * Month
             * @example 2026-10
             */
            month: string;
            /**
             * Pay
             * @description The sum of the lines' pay. Left out when no line has pay and some line lacks a rate (then shifts_missing_rate is above 0): a missing rate is never shown as zero.
             */
            pay?: string | null;
            /** Shifts Missing Rate */
            shifts_missing_rate: number;
        };
        /**
         * PlanAnswer
         * @description A plan's changes; on a dry run nothing was changed and there are no warnings.
         */
        PlanAnswer: {
            /** Changes */
            changes: components["schemas"]["PlannedChangeRead"][];
            /** Dry Run */
            dry_run: boolean;
            /** Warnings */
            warnings: components["schemas"]["WarningLine"][];
        };
        /**
         * PlannedChangeRead
         * @description One line of a plan: what happens to one shift.
         */
        PlannedChangeRead: {
            /**
             * Action
             * @enum {string}
             */
            action: "create" | "replace" | "skip" | "change" | "remove";
            /**
             * Ends At
             * Format: date-time
             */
            ends_at: string;
            /** Headcount */
            headcount: number;
            /** People */
            people: number;
            /** Shift Id */
            shift_id: string | null;
            /**
             * Shift Type Id
             * Format: uuid
             */
            shift_type_id: string;
            /**
             * Starts At
             * Format: date-time
             */
            starts_at: string;
        };
        /**
         * PremiumChanges
         * @description Any of the defaults; omitted ones stay, ``null`` and unknown fields are refused (422).
         */
        PremiumChanges: {
            /** Day Normal Minutes */
            day_normal_minutes?: number;
            /**
             * Increase1 Points
             * @example 25.00
             */
            increase1_points?: string;
            /**
             * Increase2 Points
             * @example 25.00
             */
            increase2_points?: string;
            /** Night Normal Minutes */
            night_normal_minutes?: number;
            /**
             * Overtime1 Pct
             * @example 125.00
             */
            overtime1_pct?: string;
            /**
             * Overtime2 Pct
             * @example 125.00
             */
            overtime2_pct?: string;
            /**
             * Rest Pct
             * @example 125.00
             */
            rest_pct?: string;
            rest_window?: components["schemas"]["RestWindowBody"];
            /** Tier1 Minutes */
            tier1_minutes?: number;
        };
        /**
         * PremiumOverrides
         * @description A project's own values; ``null`` returns a setting to the organisation's default.
         */
        PremiumOverrides: {
            /** Day Normal Minutes */
            day_normal_minutes?: number | null;
            /** Increase1 Points */
            increase1_points?: string | null;
            /** Increase2 Points */
            increase2_points?: string | null;
            /** Night Normal Minutes */
            night_normal_minutes?: number | null;
            /** Overtime1 Pct */
            overtime1_pct?: string | null;
            /** Overtime2 Pct */
            overtime2_pct?: string | null;
            /** Rest Pct */
            rest_pct?: string | null;
            rest_window?: components["schemas"]["RestWindowBody"] | null;
            /** Tier1 Minutes */
            tier1_minutes?: number | null;
        };
        /**
         * PremiumSettings
         * @description What a shift's hours are paid, in percent of the base rate.
         *
         *     Hours beyond the normal length (the night length on a night shift) are overtime: the first tier
         *     at ``overtime1_pct``, the rest at ``overtime2_pct``. Hours in the weekly rest are paid at
         *     ``rest_pct``; on a rest or special day, overtime adds ``increase1_points`` or
         *     ``increase2_points`` to that day's rate.
         */
        PremiumSettings: {
            /** Day Normal Minutes */
            day_normal_minutes: number;
            /**
             * Increase1 Points
             * @example 125.00
             */
            increase1_points: string;
            /**
             * Increase2 Points
             * @example 125.00
             */
            increase2_points: string;
            /** Night Normal Minutes */
            night_normal_minutes: number;
            /**
             * Overtime1 Pct
             * @example 125.00
             */
            overtime1_pct: string;
            /**
             * Overtime2 Pct
             * @example 125.00
             */
            overtime2_pct: string;
            /**
             * Rest Pct
             * @example 125.00
             */
            rest_pct: string;
            rest_window: components["schemas"]["RestWindowRead"];
            /** Tier1 Minutes */
            tier1_minutes: number;
        };
        ProblemDetail: {
            /** @description Stable machine-readable error code. */
            code: string;
            detail?: string;
            errors?: Record<string, never>[];
            field?: string;
            id?: string;
            instance?: string;
            resource?: string;
            status: number;
            title: string;
            type: string;
        };
        /**
         * ProfileUpdate
         * @description Self-service profile changes.
         *
         *     Unknown fields are accepted by the schema and refused with 403 by the service, so ``role`` is
         *     never silently dropped.
         */
        ProfileUpdate: {
            /** Full Name */
            full_name?: string;
            /** Phone */
            phone?: string | null;
        } & {
            [key: string]: unknown;
        };
        /**
         * ProjectCreateRequest
         * @description New project.
         *
         *     ``billing_rate`` must be greater than zero and ``end_date`` not before ``start_date``.
         *     ``pay_settings`` (Admin only) sets the project's pay switches and its own premium values;
         *     without it the project has pay, closed to Managers, and follows every default (005 FR-007).
         */
        ProjectCreateRequest: {
            /**
             * Billing Rate
             * @example 75.0000
             */
            billing_rate: string;
            /**
             * Client Id
             * Format: uuid
             */
            client_id: string;
            /** End Date */
            end_date?: string | null;
            /** Name */
            name: string;
            pay_settings?: components["schemas"]["ProjectPayChanges"] | null;
            /**
             * Site Id
             * Format: uuid
             */
            site_id: string;
            /**
             * Start Date
             * Format: date
             */
            start_date: string;
            type: components["schemas"]["ProjectType"];
        };
        /**
         * ProjectListResponse
         * @description The projects visible to the caller.
         */
        ProjectListResponse: {
            /** Items */
            items: components["schemas"]["ProjectRead"][];
        };
        /**
         * ProjectPayChanges
         * @description A project's switches and overrides; omitted fields stay as they are.
         */
        ProjectPayChanges: {
            /** Managers See Pay */
            managers_see_pay?: boolean;
            /** Pay Enabled */
            pay_enabled?: boolean;
            settings?: components["schemas"]["PremiumOverrides"];
        };
        /**
         * ProjectPayRead
         * @description A project's pay: whether it has pay, whether its Managers see it, and its settings.
         *
         *     ``settings`` holds the values in effect; ``own`` names those the project sets itself, the rest
         *     following the organisation's defaults.
         */
        ProjectPayRead: {
            /** Managers See Pay */
            managers_see_pay: boolean;
            /** Own */
            own: string[];
            /** Pay Enabled */
            pay_enabled: boolean;
            settings: components["schemas"]["PremiumSettings"];
        };
        /**
         * ProjectRead
         * @description A project; ``billing_rate`` is a decimal string, never a float.
         */
        ProjectRead: {
            /** Billing Rate */
            billing_rate: string;
            /**
             * Client Id
             * Format: uuid
             */
            client_id: string;
            /**
             * Created At
             * Format: date-time
             */
            created_at: string;
            /** End Date */
            end_date?: string | null;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Name */
            name: string;
            /**
             * Organization Id
             * Format: uuid
             */
            organization_id: string;
            /**
             * Site Id
             * Format: uuid
             */
            site_id: string;
            /**
             * Start Date
             * Format: date
             */
            start_date: string;
            status: components["schemas"]["ProjectStatus"];
            type: components["schemas"]["ProjectType"];
            /**
             * Updated At
             * Format: date-time
             */
            updated_at: string;
        };
        /**
         * ProjectStatus
         * @description Lifecycle state. Stored as the PostgreSQL enum ``project_status``.
         * @enum {string}
         */
        ProjectStatus: "active" | "ended";
        /**
         * ProjectType
         * @description Kind of work. Stored as the PostgreSQL enum ``project_type``; new values need a migration.
         * @enum {string}
         */
        ProjectType: "security" | "training";
        /**
         * ProjectUpdateRequest
         * @description Project fields that may change; omitted fields are left as they are, null clears.
         */
        ProjectUpdateRequest: {
            /**
             * Billing Rate
             * @example 75.0000
             */
            billing_rate?: string;
            /** End Date */
            end_date?: string | null;
            /** Name */
            name?: string;
            status?: components["schemas"]["ProjectStatus"];
        };
        /**
         * Proposal
         * @description Who works each slot, what stays empty and why, and what it changes; employees by id.
         */
        Proposal: {
            changes: components["schemas"]["Changes"];
            /** Employees */
            employees: components["schemas"]["EmployeeShare"][];
            /** No Availability */
            no_availability: string[];
            /** Short Published */
            short_published: components["schemas"]["ShortPublished"][];
            /** Slots */
            slots: components["schemas"]["ProposedSlot"][];
            /** Unfilled */
            unfilled: components["schemas"]["UnfilledSlot"][];
        };
        /**
         * ProposedSlot
         * @description A slot of the proposal: an existing draft (``shift_id``) or a new shift.
         */
        ProposedSlot: {
            /** Assigned */
            assigned: string[];
            /**
             * Date
             * Format: date
             */
            date: string;
            /**
             * Ends At
             * Format: date-time
             */
            ends_at: string;
            /** Headcount */
            headcount: number;
            /** Kept */
            kept: string[];
            /** Key */
            key: string;
            /**
             * Project Id
             * Format: uuid
             */
            project_id: string;
            /** Shift Id */
            shift_id: string | null;
            /**
             * Shift Type Id
             * Format: uuid
             */
            shift_type_id: string;
            /**
             * Starts At
             * Format: date-time
             */
            starts_at: string;
        };
        /**
         * PublishAnswer
         * @description How many drafts were published.
         */
        PublishAnswer: {
            /** Published */
            published: number;
        };
        /**
         * PublishPreviewRead
         * @description What publishing the project's drafts on those dates would make live (FR-036).
         */
        PublishPreviewRead: {
            /** Filled */
            filled: number;
            /** Open Warnings */
            open_warnings: number;
            /** People */
            people: number;
            /** Required */
            required: number;
            /** Shifts */
            shifts: number;
            /** Unfilled */
            unfilled: number;
        };
        /**
         * PublishRequest
         * @description The dates, at the project's site, whose drafts to publish.
         *
         *     ``notify`` is the publish review's choice between Notify everyone (the default) and Publish
         *     silently; one event per affected person is recorded either way, for feature 010 to deliver
         *     (007 FR-036, research R12).
         */
        PublishRequest: {
            /**
             * From
             * Format: date
             */
            from: string;
            /**
             * Notify
             * @default true
             */
            notify: boolean;
            /**
             * To
             * Format: date
             */
            to: string;
        };
        /**
         * QualificationListResponse
         * @description The organisation's qualifications.
         */
        QualificationListResponse: {
            /** Items */
            items: components["schemas"]["hds__routers__qualifications__QualificationRead"][];
        };
        /**
         * QualificationRenameRequest
         * @description The new name; unique within the organisation. Any other field is refused with 422.
         */
        QualificationRenameRequest: {
            /** Name */
            name: string;
        };
        /**
         * QualificationWriteRequest
         * @description Qualification name for create; unique within the organisation.
         */
        QualificationWriteRequest: {
            /** Name */
            name: string;
        };
        /**
         * Reason
         * @description One reason places stayed empty, and how many candidates it excluded.
         */
        Reason: {
            /** Code */
            code: string;
            /** Employees */
            employees: number;
        };
        /**
         * RefreshRequest
         * @description A refresh token to exchange or revoke.
         */
        RefreshRequest: {
            /** Refresh Token */
            refresh_token: string;
        };
        /**
         * RestWindowBody
         * @description The weekly rest, from a weekday and local time to a weekday and local time at the site.
         *
         *     Weekdays run from 0 (Sunday) to 6 (Saturday); the window is read forward from its start, and
         *     its ends must differ (422 ``pay.rest_window_invalid``).
         */
        RestWindowBody: {
            /**
             * End Time
             * @example 07:00
             */
            end_time: string;
            /**
             * End Weekday
             * @description 0 Sunday to 6 Saturday.
             */
            end_weekday: number;
            /**
             * Start Time
             * @example 07:00
             */
            start_time: string;
            /**
             * Start Weekday
             * @description 0 Sunday to 6 Saturday.
             */
            start_weekday: number;
        };
        /**
         * RestWindowRead
         * @description The weekly rest; times are local at the site, written ``HH:MM:SS``.
         */
        RestWindowRead: {
            /**
             * End Time
             * @example 07:00
             */
            end_time: string;
            /** End Weekday */
            end_weekday: number;
            /**
             * Start Time
             * @example 07:00
             */
            start_time: string;
            /** Start Weekday */
            start_weekday: number;
        };
        /**
         * SchedulingSettings
         * @description The limits the roster is generated and checked against, in whole hours.
         */
        SchedulingSettings: {
            /** Max Weekly Hours */
            max_weekly_hours: number;
            /** Min Rest Hours */
            min_rest_hours: number;
        };
        /**
         * SchedulingSettingsUpdate
         * @description Either limit; any other field is refused with 422.
         */
        SchedulingSettingsUpdate: {
            /** Max Weekly Hours */
            max_weekly_hours?: number;
            /** Min Rest Hours */
            min_rest_hours?: number;
        };
        /**
         * SessionRead
         * @description One signed-in device: its live refresh token, never the token itself.
         *
         *     ``last_refreshed_at`` is when the device last signed in or refreshed; ``expires_at`` when the
         *     session ends unless it refreshes again.
         */
        SessionRead: {
            /**
             * Client Kind
             * @enum {string}
             */
            client_kind: "web" | "native";
            /**
             * Expires At
             * Format: date-time
             */
            expires_at: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /**
             * Last Refreshed At
             * Format: date-time
             */
            last_refreshed_at: string;
        };
        /**
         * ShiftAnswer
         * @description A stored shift and the warnings its change caused.
         */
        ShiftAnswer: {
            shift: components["schemas"]["ShiftRead"];
            /** Warnings */
            warnings: components["schemas"]["WarningRead"][];
        };
        /**
         * ShiftContact
         * @description Who to contact about a shift: one of its project's managers.
         */
        ShiftContact: {
            /** Email */
            email: string;
            /** Full Name */
            full_name: string;
            /** Phone */
            phone: string | null;
        };
        /**
         * ShiftCreateRequest
         * @description A new shift of one of the project's shift types on ``date``.
         *
         *     Its times are the type's local times at the project's site unless ``starts_at`` and
         *     ``ends_at`` are both given. ``pay_rate`` is the hourly pay for it, otherwise each employee's own
         *     rate applies.
         */
        ShiftCreateRequest: {
            /**
             * Date
             * Format: date
             */
            date: string;
            /** Ends At */
            ends_at?: string | null;
            /**
             * Headcount
             * @description How many employees the shift needs.
             */
            headcount: number;
            /** Pay Rate */
            pay_rate?: string | null;
            /**
             * Project Id
             * Format: uuid
             */
            project_id: string;
            /**
             * Shift Type Id
             * Format: uuid
             */
            shift_type_id: string;
            /** Starts At */
            starts_at?: string | null;
        };
        /**
         * ShiftPage
         * @description A page of the roster; ``next_cursor`` continues it, ``null`` on the last page.
         */
        ShiftPage: {
            /** Items */
            items: components["schemas"]["ShiftRead"][];
            /** Next Cursor */
            next_cursor: string | null;
        };
        /**
         * ShiftRead
         * @description A shift: UTC instants, its site's IANA time zone, and who is on it.
         */
        ShiftRead: {
            /** Assignments */
            assignments: components["schemas"]["AssignmentRead"][];
            /**
             * Ends At
             * Format: date-time
             */
            ends_at: string;
            /** Etag */
            etag: string;
            /** Headcount */
            headcount: number;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Pay Rate */
            pay_rate: string | null;
            /**
             * Project Id
             * Format: uuid
             */
            project_id: string;
            /** Published At */
            published_at: string | null;
            /**
             * Shift Type Id
             * Format: uuid
             */
            shift_type_id: string;
            /**
             * Starts At
             * Format: date-time
             */
            starts_at: string;
            /**
             * Status
             * @enum {string}
             */
            status: "draft" | "published";
            /** Timezone */
            timezone: string;
        };
        /**
         * ShiftTypeCreateRequest
         * @description A new shift type; it crosses midnight when the end is not after the start.
         */
        ShiftTypeCreateRequest: {
            /**
             * Ends At Local
             * @example 07:00
             */
            ends_at_local: string;
            /** Name */
            name: string;
            /**
             * Starts At Local
             * @example 07:00
             */
            starts_at_local: string;
        };
        /**
         * ShiftTypeIdsRequest
         * @description The complete set of shift type ids a project uses; an empty list clears it.
         */
        ShiftTypeIdsRequest: {
            /** Ids */
            ids: string[];
        };
        /**
         * ShiftTypeListResponse
         * @description Shift types.
         */
        ShiftTypeListResponse: {
            /** Items */
            items: components["schemas"]["ShiftTypeRead"][];
        };
        /**
         * ShiftTypeRead
         * @description A shift type; its times are local to the site a shift is at.
         */
        ShiftTypeRead: {
            /**
             * Created At
             * Format: date-time
             */
            created_at: string;
            /** Crosses Midnight */
            crosses_midnight: boolean;
            /**
             * Ends At Local
             * @example 07:00
             */
            ends_at_local: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Is Active */
            is_active: boolean;
            /** Name */
            name: string;
            /**
             * Starts At Local
             * @example 07:00
             */
            starts_at_local: string;
            /**
             * Updated At
             * Format: date-time
             */
            updated_at: string;
        };
        /**
         * ShiftTypeUpdateRequest
         * @description Shift type fields that may change; ``is_active`` false retires it.
         */
        ShiftTypeUpdateRequest: {
            /**
             * Ends At Local
             * @example 07:00
             */
            ends_at_local?: string;
            /** Is Active */
            is_active?: boolean;
            /** Name */
            name?: string;
            /**
             * Starts At Local
             * @example 07:00
             */
            starts_at_local?: string;
        };
        /**
         * ShiftUpdateRequest
         * @description Shift fields that may change; null clears ``pay_rate``.
         */
        ShiftUpdateRequest: {
            /**
             * Ends At
             * Format: date-time
             */
            ends_at?: string;
            /** Headcount */
            headcount?: number;
            /** Pay Rate */
            pay_rate?: string | null;
            /**
             * Starts At
             * Format: date-time
             */
            starts_at?: string;
        };
        /**
         * ShortPublished
         * @description A published shift with fewer people than its need; reported, not changed.
         */
        ShortPublished: {
            /** Assigned */
            assigned: number;
            /** Needed */
            needed: number;
            /**
             * Shift Id
             * Format: uuid
             */
            shift_id: string;
        };
        /**
         * SiteCreateRequest
         * @description New site; ``timezone`` is an IANA zone name such as ``Asia/Jerusalem``.
         */
        SiteCreateRequest: {
            /** Address */
            address: string;
            /** Name */
            name: string;
            /** Timezone */
            timezone: string;
        };
        /**
         * SiteListResponse
         * @description The organisation's sites.
         */
        SiteListResponse: {
            /** Items */
            items: components["schemas"]["SiteRead"][];
        };
        /**
         * SiteRead
         * @description A site with its IANA time zone.
         */
        SiteRead: {
            /** Address */
            address: string;
            /**
             * Created At
             * Format: date-time
             */
            created_at: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Name */
            name: string;
            /**
             * Organization Id
             * Format: uuid
             */
            organization_id: string;
            /** Timezone */
            timezone: string;
            /**
             * Updated At
             * Format: date-time
             */
            updated_at: string;
        };
        /**
         * SiteUpdateRequest
         * @description Site fields that may change; omitted fields are left as they are.
         */
        SiteUpdateRequest: {
            /** Address */
            address?: string;
            /** Name */
            name?: string;
            /** Timezone */
            timezone?: string;
        };
        /**
         * SpecialDayChange
         * @description A day's date, name or rate; omitted fields stay, ``null`` and unknown fields are refused.
         */
        SpecialDayChange: {
            /**
             * Day
             * Format: date
             */
            day?: string;
            /** Name */
            name?: string;
            /**
             * Rate Pct
             * @example 125.00
             */
            rate_pct?: string;
        };
        /**
         * SpecialDayCreate
         * @description A date paid at its own rate (150.00 percent unless given), with a name.
         */
        SpecialDayCreate: {
            /**
             * Day
             * Format: date
             */
            day: string;
            /** Name */
            name: string;
            /** Rate Pct */
            rate_pct?: string | null;
        };
        /**
         * SpecialDayList
         * @description The organisation's days, by date.
         */
        SpecialDayList: {
            /** Items */
            items: components["schemas"]["SpecialDayRead"][];
        };
        /**
         * SpecialDayRead
         * @description A special day; ``project_id`` is set for a project's own day.
         */
        SpecialDayRead: {
            /**
             * Day
             * Format: date
             */
            day: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Name */
            name: string;
            /** Project Id */
            project_id: string | null;
            /**
             * Rate Pct
             * @example 125.00
             */
            rate_pct: string;
        };
        /**
         * SubmissionRead
         * @description Where the caller's availability for one window stands.
         *
         *     ``status`` is derived: ``locked`` once the deadline has passed; otherwise ``submitted`` while
         *     the entries equal what was submitted, ``draft`` after any change or entry, ``not_submitted``
         *     with neither. ``counted_unavailable`` is true after the deadline when nothing was submitted:
         *     the employee is then not scheduled anywhere in the window. ``submitted_by`` is who made the
         *     latest submit: the employee, or an Admin who submitted on their behalf.
         */
        SubmissionRead: {
            /** Counted Unavailable */
            counted_unavailable: boolean;
            /**
             * Deadline
             * Format: date-time
             */
            deadline: string;
            /**
             * Status
             * @enum {string}
             */
            status: "not_submitted" | "draft" | "submitted" | "locked";
            /** Submitted At */
            submitted_at: string | null;
            /** Submitted By */
            submitted_by: string | null;
            /**
             * Window Id
             * Format: uuid
             */
            window_id: string;
        };
        /**
         * SubmitRequest
         * @description The window to submit for.
         */
        SubmitRequest: {
            /**
             * Window Id
             * Format: uuid
             */
            window_id: string;
        };
        /**
         * TemplateApply
         * @description Apply a saved week to the week starting on Sunday ``week``.
         */
        TemplateApply: {
            /**
             * Dry Run
             * @default false
             */
            dry_run: boolean;
            /**
             * On Conflict
             * @default allow
             * @enum {string}
             */
            on_conflict: "allow" | "overwrite" | "unfilled";
            /**
             * Week
             * Format: date
             */
            week: string;
        };
        /**
         * TemplateCreate
         * @description Save the project's week starting on Sunday ``week`` under ``name``.
         */
        TemplateCreate: {
            /** Name */
            name: string;
            /**
             * Week
             * Format: date
             */
            week: string;
        };
        /**
         * TemplateList
         * @description The project's saved weeks, by name.
         */
        TemplateList: {
            /** Items */
            items: components["schemas"]["TemplateRead"][];
        };
        /**
         * TemplateRead
         * @description A saved week: its name and how many shifts it holds.
         */
        TemplateRead: {
            /**
             * Created At
             * Format: date-time
             */
            created_at: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Name */
            name: string;
            /** Shifts */
            shifts: number;
        };
        /**
         * TokenResponse
         * @description Token pair returned by sign-in and refresh.
         *
         *     ``expires_in`` is the access-token lifetime in seconds.
         */
        TokenResponse: {
            /** Access Token */
            access_token: string;
            /** Expires In */
            expires_in: number;
            /** Refresh Token */
            refresh_token: string;
            /**
             * Token Type
             * @default bearer
             */
            token_type: string;
        };
        /**
         * TrackedRead
         * @description One employee's availability for the window.
         */
        TrackedRead: {
            /** Counted Unavailable */
            counted_unavailable: boolean;
            /** Full Name */
            full_name: string;
            /**
             * Status
             * @enum {string}
             */
            status: "not_submitted" | "draft" | "submitted" | "locked";
            /** Submitted At */
            submitted_at: string | null;
            /**
             * User Id
             * Format: uuid
             */
            user_id: string;
        };
        /**
         * TrackerRead
         * @description Every active staff member's availability for the window, by name (FR-040).
         */
        TrackerRead: {
            /** Items */
            items: components["schemas"]["TrackedRead"][];
        };
        /**
         * UnfilledSlot
         * @description A slot with empty places, and why.
         */
        UnfilledSlot: {
            /** Key */
            key: string;
            /** Places */
            places: number;
            /** Reasons */
            reasons: components["schemas"]["Reason"][];
        };
        /**
         * UserCreateRequest
         * @description New employee.
         *
         *     The initial password needs at least 12 characters; the role defaults to employee and the
         *     hourly rate is optional.
         */
        UserCreateRequest: {
            /** Default Hourly Rate */
            default_hourly_rate?: string | null;
            /**
             * Email
             * Format: email
             */
            email: string;
            /** Full Name */
            full_name: string;
            /** Initial Password */
            initial_password: string;
            /** Phone */
            phone?: string | null;
            /**
             * Role
             * @default employee
             * @enum {string}
             */
            role: "employee" | "manager" | "admin";
        };
        /**
         * UserListResponse
         * @description Employees matching a search; ``total`` is the number of items returned.
         */
        UserListResponse: {
            /** Items */
            items: components["schemas"]["UserRead"][];
            /** Total */
            total: number;
        };
        /**
         * UserRead
         * @description A user account as returned by the API.
         *
         *     ``default_hourly_rate`` is a decimal string and is view-only for the user it belongs to
         *     (FR-017).
         */
        UserRead: {
            /**
             * Created At
             * Format: date-time
             */
            created_at: string;
            /** Default Hourly Rate */
            default_hourly_rate?: string | null;
            /**
             * Email
             * Format: email
             */
            email: string;
            /** Full Name */
            full_name: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Is Active */
            is_active: boolean;
            /**
             * Organization Id
             * Format: uuid
             */
            organization_id: string;
            /** Phone */
            phone?: string | null;
            role: components["schemas"]["UserRole"];
            /**
             * Updated At
             * Format: date-time
             */
            updated_at: string;
        };
        /**
         * UserRole
         * @description Role in the permission matrix (Employee, Manager, Admin, Client).
         *
         *     Stored as the PostgreSQL enum ``user_role``; adding a value needs a migration.
         * @enum {string}
         */
        UserRole: "employee" | "manager" | "admin" | "client";
        /**
         * UserUpdateRequest
         * @description Fields a Manager or Admin may change; omitted fields are left as they are, null clears.
         */
        UserUpdateRequest: {
            /** Default Hourly Rate */
            default_hourly_rate?: string | null;
            /** Full Name */
            full_name?: string;
            /** Phone */
            phone?: string | null;
            /**
             * Role
             * @enum {string}
             */
            role?: "employee" | "manager" | "admin";
        };
        /**
         * WarningLine
         * @description A warning applying a plan caused; nothing was refused.
         */
        WarningLine: {
            /** Code */
            code: string;
            /** Detail */
            detail: string;
            /** Employee Id */
            employee_id: string | null;
            /** Shift Ids */
            shift_ids: string[];
        };
        /**
         * WarningRead
         * @description A rule a change breaks; the change was stored anyway.
         */
        WarningRead: {
            /** Code */
            code: string;
            /** Detail */
            detail: string;
            /** Employee Id */
            employee_id: string | null;
            /** Shift Ids */
            shift_ids: string[];
        };
        /**
         * WarningsAnswer
         * @description The warnings a change leaves, and the shift's new ``etag``.
         */
        WarningsAnswer: {
            /** Shift Etag */
            shift_etag: string;
            /** Warnings */
            warnings: components["schemas"]["WarningRead"][];
        };
        /**
         * WindowCreateRequest
         * @description A period of dates (inclusive) and the deadline for submitting availability for it.
         */
        WindowCreateRequest: {
            /**
             * Deadline
             * Format: date-time
             */
            deadline: string;
            /**
             * Ends On
             * Format: date
             */
            ends_on: string;
            /**
             * Starts On
             * Format: date
             */
            starts_on: string;
        };
        /**
         * WindowListResponse
         * @description Windows in date order.
         */
        WindowListResponse: {
            /** Items */
            items: components["schemas"]["WindowRead"][];
        };
        /**
         * WindowRead
         * @description A submission window; ``is_open`` is true while now is before ``deadline``.
         */
        WindowRead: {
            /**
             * Deadline
             * Format: date-time
             */
            deadline: string;
            /**
             * Ends On
             * Format: date
             */
            ends_on: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Is Open */
            is_open: boolean;
            /**
             * Starts On
             * Format: date
             */
            starts_on: string;
        };
        /**
         * WindowUpdateRequest
         * @description A new deadline.
         */
        WindowUpdateRequest: {
            /**
             * Deadline
             * Format: date-time
             */
            deadline?: string;
        };
        /**
         * WorkLogRead
         * @description Who worked in the range, once per person and project, by name; never any pay.
         */
        WorkLogRead: {
            /**
             * From
             * Format: date
             */
            from: string;
            /** Rows */
            rows: components["schemas"]["WorkLogRowRead"][];
            /**
             * To
             * Format: date
             */
            to: string;
        };
        /**
         * WorkLogRowRead
         * @description One person at one project within the range.
         */
        WorkLogRowRead: {
            /**
             * First Date
             * Format: date
             */
            first_date: string;
            /** Full Name */
            full_name: string;
            /**
             * Hours
             * @example 7.50
             */
            hours: string;
            /**
             * Last Date
             * Format: date
             */
            last_date: string;
            /**
             * Project Id
             * Format: uuid
             */
            project_id: string;
            /** Project Name */
            project_name: string;
            /** Shifts */
            shifts: number;
            /**
             * User Id
             * Format: uuid
             */
            user_id: string;
        };
        /**
         * GroupRead
         * @description A group a user belongs to.
         */
        hds__routers__employees__GroupRead: {
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Name */
            name: string;
        };
        /**
         * QualificationRead
         * @description A qualification a user holds.
         */
        hds__routers__employees__QualificationRead: {
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Name */
            name: string;
        };
        /**
         * GroupRead
         * @description A group with its audit timestamps.
         */
        hds__routers__groups__GroupRead: {
            /**
             * Created At
             * Format: date-time
             */
            created_at: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Name */
            name: string;
            /**
             * Organization Id
             * Format: uuid
             */
            organization_id: string;
            /**
             * Updated At
             * Format: date-time
             */
            updated_at: string;
        };
        /**
         * QualificationRead
         * @description A qualification with its audit timestamps.
         */
        hds__routers__qualifications__QualificationRead: {
            /**
             * Created At
             * Format: date-time
             */
            created_at: string;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Name */
            name: string;
            /**
             * Organization Id
             * Format: uuid
             */
            organization_id: string;
            /**
             * Updated At
             * Format: date-time
             */
            updated_at: string;
        };
    };
    responses: never;
    parameters: never;
    requestBodies: never;
    headers: never;
    pathItems: never;
}
export type $defs = Record<string, never>;
export interface operations {
    unassign_api_v1_assignments__id__delete: {
        parameters: {
            query?: never;
            header?: {
                /** @description The shift's `etag` as read; a stale one is refused with 412 (`roster.changed`). */
                "If-Match"?: string | null;
            };
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["WarningsAnswer"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The record changed since it was read: the If-Match header is stale (roster.changed). */
            412: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    change_assignment_times_api_v1_assignments__id__patch: {
        parameters: {
            query?: never;
            header?: {
                /** @description The shift's `etag` as read; a stale one is refused with 412 (`roster.changed`). */
                "If-Match"?: string | null;
            };
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["AssignmentTimesRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AssignmentAnswer"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The record changed since it was read: the If-Match header is stale (roster.changed). */
            412: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    override_api_v1_assignments__id__overrides_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["OverrideRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["OverrideCreated"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    list_windows_api_v1_availability_windows_get: {
        parameters: {
            query?: {
                from?: string | null;
                to?: string | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["WindowListResponse"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    open_window_api_v1_availability_windows_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["WindowCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["WindowRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    remove_window_api_v1_availability_windows__id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    change_window_api_v1_availability_windows__id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["WindowUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["WindowRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    tracker_api_v1_availability_windows__id__submissions_get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["TrackerRead"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    list_clients_api_v1_clients_get: {
        parameters: {
            query?: {
                is_active?: boolean | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ClientListResponse"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    create_client_api_v1_clients_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ClientCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ClientRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    get_client_api_v1_clients__id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ClientRead"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    delete_client_api_v1_clients__id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    edit_client_api_v1_clients__id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ClientUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ClientRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    list_client_users_api_v1_clients__id__users_get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ClientUserRead"][];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    create_client_user_api_v1_clients__id__users_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ClientUserCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ClientUserRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The password-hashing queue is full; try again after the Retry-After interval. */
            503: {
                headers: {
                    /** @description Seconds to wait before trying again. */
                    "Retry-After"?: number;
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    remove_client_user_api_v1_clients__id__users__user_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
                user_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    remove_entry_api_v1_constraints__id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    change_entry_api_v1_constraints__id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["EntryUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["EntryRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    list_employees_api_v1_employees_get: {
        parameters: {
            query?: {
                search?: string | null;
                role?: components["schemas"]["UserRole"] | null;
                is_active?: boolean | null;
                group_id?: string | null;
                qualification_id?: string | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["UserListResponse"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    create_employee_api_v1_employees_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["UserCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["UserRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The password-hashing queue is full; try again after the Retry-After interval. */
            503: {
                headers: {
                    /** @description Seconds to wait before trying again. */
                    "Retry-After"?: number;
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    get_employee_api_v1_employees__id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["UserRead"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    delete_employee_api_v1_employees__id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    edit_employee_api_v1_employees__id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["UserUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["UserRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    submit_on_behalf_api_v1_employees__id__availability_submissions_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["SubmitRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SubmissionRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    list_entries_api_v1_employees__id__constraints_get: {
        parameters: {
            query?: {
                from?: string | null;
                to?: string | null;
            };
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["EntryListResponse"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    add_entry_api_v1_employees__id__constraints_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["EntryCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["EntryRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    deactivate_employee_api_v1_employees__id__deactivate_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["UserRead"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    get_employee_groups_api_v1_employees__id__groups_get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["hds__routers__employees__GroupRead"][];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    replace_employee_groups_api_v1_employees__id__groups_put: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["IDListRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    get_employee_qualifications_api_v1_employees__id__qualifications_get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["hds__routers__employees__QualificationRead"][];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    replace_employee_qualifications_api_v1_employees__id__qualifications_put: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["IDListRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    reactivate_employee_api_v1_employees__id__reactivate_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["UserRead"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    list_groups_api_v1_groups_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["GroupListResponse"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    create_group_api_v1_groups_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["GroupWriteRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["hds__routers__groups__GroupRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    delete_group_api_v1_groups__id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    rename_group_api_v1_groups__id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["GroupRenameRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["hds__routers__groups__GroupRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    read_contact_api_v1_organisation_contact_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["OrganisationContact"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    change_contact_api_v1_organisation_contact_patch: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["OrganisationContactUpdate"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["OrganisationContact"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    read_defaults_api_v1_organisation_pay_settings_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["PremiumSettings"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    change_defaults_api_v1_organisation_pay_settings_patch: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["PremiumChanges"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["PremiumSettings"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    read_settings_api_v1_organisation_scheduling_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SchedulingSettings"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    change_settings_api_v1_organisation_scheduling_patch: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["SchedulingSettingsUpdate"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SchedulingSettings"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    list_projects_api_v1_projects_get: {
        parameters: {
            query?: {
                status?: components["schemas"]["ProjectStatus"] | null;
                client_id?: string | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ProjectListResponse"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    create_project_api_v1_projects_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ProjectCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ProjectRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    get_project_api_v1_projects__id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ProjectRead"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    delete_project_api_v1_projects__id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    edit_project_api_v1_projects__id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ProjectUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ProjectRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    copy_week_api_v1_projects__id__copy_week_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["CopyWeekRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["PlanAnswer"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    list_managers_api_v1_projects__id__managers_get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ManagersResponse"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    replace_managers_api_v1_projects__id__managers_put: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ManagersRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ManagersResponse"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    read_project_pay_api_v1_projects__id__pay_settings_get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ProjectPayRead"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    change_project_pay_api_v1_projects__id__pay_settings_patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ProjectPayChanges"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ProjectPayRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    publish_api_v1_projects__id__publish_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["PublishRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["PublishAnswer"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    publish_preview_api_v1_projects__id__publish_preview_get: {
        parameters: {
            query: {
                from: string;
                to: string;
            };
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["PublishPreviewRead"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    list_needs_api_v1_projects__id__requirements_get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["NeedsResponse"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    replace_needs_api_v1_projects__id__requirements_put: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["NeedsRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["NeedsResponse"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    bulk_api_v1_projects__id__roster_bulk_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["BulkRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["PlanAnswer"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    project_types_api_v1_projects__id__shift_types_get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ShiftTypeListResponse"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    set_project_types_api_v1_projects__id__shift_types_put: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ShiftTypeIdsRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ShiftTypeListResponse"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    list_project_days_api_v1_projects__id__special_days_get: {
        parameters: {
            query?: {
                /** @description Only the days of this year. */
                year?: number | null;
            };
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["DaysInEffect"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    add_project_day_api_v1_projects__id__special_days_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["SpecialDayCreate"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SpecialDayRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    list_templates_api_v1_projects__id__week_templates_get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["TemplateList"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    save_template_api_v1_projects__id__week_templates_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["TemplateCreate"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["TemplateRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    delete_template_api_v1_projects__id__week_templates__template_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
                template_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    apply_template_api_v1_projects__id__week_templates__template_id__apply_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
                template_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["TemplateApply"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["PlanAnswer"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    list_qualifications_api_v1_qualifications_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["QualificationListResponse"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    create_qualification_api_v1_qualifications_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["QualificationWriteRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["hds__routers__qualifications__QualificationRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    delete_qualification_api_v1_qualifications__id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    rename_qualification_api_v1_qualifications__id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["QualificationRenameRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["hds__routers__qualifications__QualificationRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    hours_api_v1_reports_hours_get: {
        parameters: {
            query: {
                from: string;
                to: string;
                group_by: "employee" | "project";
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HoursReportRead"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    work_log_api_v1_reports_work_log_get: {
        parameters: {
            query?: {
                project_id?: string | null;
                site_id?: string | null;
                group_id?: string | null;
                from?: string | null;
                to?: string | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["WorkLogRead"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    list_jobs_api_v1_schedule_jobs_get: {
        parameters: {
            query?: {
                status?: ("queued" | "running" | "succeeded" | "failed") | null;
                cursor?: string | null;
                limit?: number;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["JobPage"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    create_job_api_v1_schedule_jobs_post: {
        parameters: {
            query?: never;
            header?: {
                "idempotency-key"?: string | null;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["JobCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            202: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["JobRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    read_job_api_v1_schedule_jobs__id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["JobRead"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    accept_job_api_v1_schedule_jobs__id__accept_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AcceptAnswer"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    discard_job_api_v1_schedule_jobs__id__discard_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["JobRead"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    list_types_api_v1_shift_types_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ShiftTypeListResponse"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    add_type_api_v1_shift_types_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ShiftTypeCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ShiftTypeRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    change_type_api_v1_shift_types__id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ShiftTypeUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ShiftTypeRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    list_shifts_api_v1_shifts_get: {
        parameters: {
            query?: {
                from?: string | null;
                to?: string | null;
                project_id?: string | null;
                status?: ("draft" | "published") | null;
                cursor?: string | null;
                limit?: number;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ShiftPage"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    create_shift_api_v1_shifts_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ShiftCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ShiftAnswer"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    delete_shift_api_v1_shifts__id__delete: {
        parameters: {
            query?: never;
            header?: {
                /** @description The shift's `etag` as read; a stale one is refused with 412 (`roster.changed`). */
                "If-Match"?: string | null;
            };
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The record changed since it was read: the If-Match header is stale (roster.changed). */
            412: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    change_shift_api_v1_shifts__id__patch: {
        parameters: {
            query?: never;
            header?: {
                /** @description The shift's `etag` as read; a stale one is refused with 412 (`roster.changed`). */
                "If-Match"?: string | null;
            };
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ShiftUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ShiftAnswer"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The record changed since it was read: the If-Match header is stale (roster.changed). */
            412: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    assign_api_v1_shifts__id__assignments_post: {
        parameters: {
            query?: never;
            header?: {
                "idempotency-key"?: string | null;
                /** @description The shift's `etag` as read; a stale one is refused with 412 (`roster.changed`). */
                "If-Match"?: string | null;
            };
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["AssignRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AssignmentAnswer"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The record changed since it was read: the If-Match header is stale (roster.changed). */
            412: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    list_sites_api_v1_sites_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SiteListResponse"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    create_site_api_v1_sites_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["SiteCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SiteRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    get_site_api_v1_sites__id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SiteRead"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    edit_site_api_v1_sites__id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["SiteUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SiteRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    list_days_api_v1_special_days_get: {
        parameters: {
            query?: {
                /** @description Only the days of this year. */
                year?: number | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SpecialDayList"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    add_day_api_v1_special_days_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["SpecialDayCreate"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SpecialDayRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    delete_day_api_v1_special_days__id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    change_day_api_v1_special_days__id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["SpecialDayChange"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SpecialDayRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    create_invitation_auth_invite_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["InviteRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            202: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The password-hashing queue is full; try again after the Retry-After interval. */
            503: {
                headers: {
                    /** @description Seconds to wait before trying again. */
                    "Retry-After"?: number;
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    accept_invitation_auth_invite_accept_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["AcceptInviteRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The password-hashing queue is full; try again after the Retry-After interval. */
            503: {
                headers: {
                    /** @description Seconds to wait before trying again. */
                    "Retry-After"?: number;
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    login_auth_login_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["LoginRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["TokenResponse"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The account is temporarily locked after repeated failed sign-ins. */
            423: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Too many failed sign-ins from this network; retry after the Retry-After interval. */
            429: {
                headers: {
                    /** @description Seconds to wait before trying again. */
                    "Retry-After"?: number;
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The password-hashing queue is full; try again after the Retry-After interval. */
            503: {
                headers: {
                    /** @description Seconds to wait before trying again. */
                    "Retry-After"?: number;
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    logout_auth_logout_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["RefreshRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    logout_all_auth_logout_all_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    confirm_password_reset_auth_password_reset_confirm_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["PasswordResetConfirm"];
            };
        };
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The password-hashing queue is full; try again after the Retry-After interval. */
            503: {
                headers: {
                    /** @description Seconds to wait before trying again. */
                    "Retry-After"?: number;
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    request_password_reset_auth_password_reset_request_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["PasswordResetRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            202: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    refresh_token_auth_refresh_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["RefreshRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["TokenResponse"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    get_me_me_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["MeRead"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    patch_me_me_patch: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ProfileUpdate"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["UserRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    read_submission_me_availability_submissions_get: {
        parameters: {
            query: {
                window_id: string;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SubmissionRead"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    submit_me_availability_submissions_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["SubmitRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SubmissionRead"];
                };
            };
            /** @description The request body could not be parsed. */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request conflicts with the current state (duplicate, last admin, in use). */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    my_pay_me_pay_get: {
        parameters: {
            query?: {
                month?: string | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["PayToDateRead"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    list_sessions_me_sessions_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SessionRead"][];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    end_session_me_sessions__id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    my_shifts_me_shifts_get: {
        parameters: {
            query?: {
                from?: string | null;
                to?: string | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["MyShiftList"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
    my_shift_me_shifts__id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["MyShiftDetail"];
                };
            };
            /** @description Missing, invalid or expired credentials. */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description Authenticated but the role or data scope does not allow this. */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The referenced resource does not exist in the caller's organisation. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
            /** @description The request is invalid. */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/problem+json": components["schemas"]["ProblemDetail"];
                };
            };
        };
    };
}
