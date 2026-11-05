"""Pay settings router: the organisation's premium defaults and each project's pay settings.

Layer: routers. Delegates to ``PaySettingsService``, which ``hds.wiring`` builds; HTTP only.
Managers and Admins read; Admins change (005 FR-005 to FR-007).
"""

import uuid
from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, StrictBool

from hds.core.deps import require_admin, require_manager
from hds.core.types import (
    ChangeRequest,
    IsoTime,
    LocalTimeOut,
    Minutes,
    Percent,
    PercentOut,
    Points,
    Weekday,
    omittable,
)
from hds.models.user import User
from hds.payroll import PaySettings, RestWindow
from hds.services.pay_settings import PaySettingsService, ProjectPay
from hds.wiring import pay_settings

router = APIRouter(prefix="/api/v1", tags=["pay settings"])


class RestWindowBody(BaseModel):
    """The weekly rest, from a weekday and local time to a weekday and local time at the site.

    Weekdays run from 0 (Sunday) to 6 (Saturday); the window is read forward from its start, and
    its ends must differ (422 ``pay.rest_window_invalid``).
    """

    start_weekday: Weekday
    start_time: IsoTime
    end_weekday: Weekday
    end_time: IsoTime

    model_config = ConfigDict(extra="forbid")

    def window(self) -> RestWindow:
        """The window as the pay package takes it."""
        return RestWindow(self.start_weekday, self.start_time, self.end_weekday, self.end_time)


class RestWindowRead(BaseModel):
    """The weekly rest; times are local at the site, written ``HH:MM:SS``."""

    start_weekday: int
    start_time: LocalTimeOut
    end_weekday: int
    end_time: LocalTimeOut


class PremiumSettings(BaseModel):
    """What a shift's hours are paid, in percent of the base rate.

    Hours beyond the normal length (the night length on a night shift) are overtime: the first tier
    at ``overtime1_pct``, the rest at ``overtime2_pct``. Hours in the weekly rest are paid at
    ``rest_pct``; on a rest or special day, overtime adds ``increase1_points`` or
    ``increase2_points`` to that day's rate.
    """

    day_normal_minutes: int
    night_normal_minutes: int
    tier1_minutes: int
    overtime1_pct: PercentOut
    overtime2_pct: PercentOut
    rest_window: RestWindowRead
    rest_pct: PercentOut
    increase1_points: PercentOut
    increase2_points: PercentOut


class PremiumChanges(ChangeRequest):
    """Any of the defaults; omitted ones stay, ``null`` and unknown fields are refused (422)."""

    day_normal_minutes: Minutes = omittable()
    night_normal_minutes: Minutes = omittable()
    tier1_minutes: Minutes = omittable()
    overtime1_pct: Percent = omittable()
    overtime2_pct: Percent = omittable()
    rest_window: RestWindowBody = omittable()
    rest_pct: Percent = omittable()
    increase1_points: Points = omittable()
    increase2_points: Points = omittable()


class PremiumOverrides(ChangeRequest):
    """A project's own values; ``null`` returns a setting to the organisation's default."""

    day_normal_minutes: Minutes | None = None
    night_normal_minutes: Minutes | None = None
    tier1_minutes: Minutes | None = None
    overtime1_pct: Percent | None = None
    overtime2_pct: Percent | None = None
    rest_window: RestWindowBody | None = None
    rest_pct: Percent | None = None
    increase1_points: Points | None = None
    increase2_points: Points | None = None


class ProjectPayChanges(ChangeRequest):
    """A project's switches and overrides; omitted fields stay as they are."""

    pay_enabled: StrictBool = omittable()
    managers_see_pay: StrictBool = omittable()
    settings: PremiumOverrides = omittable()


class ProjectPayRead(BaseModel):
    """A project's pay: whether it has pay, whether its Managers see it, and its settings.

    ``settings`` holds the values in effect; ``own`` names those the project sets itself, the rest
    following the organisation's defaults.
    """

    pay_enabled: bool
    managers_see_pay: bool
    settings: PremiumSettings
    own: list[str]


def changes_of(body: BaseModel) -> dict[str, Any]:
    """The fields a body sent, with a rest window turned into the pay package's type."""
    data: dict[str, Any] = {}
    for name in body.model_fields_set:
        value = getattr(body, name)
        data[name] = value.window() if isinstance(value, RestWindowBody) else value
    return data


def project_pay_changes(body: ProjectPayChanges) -> dict[str, Any]:
    """A project's pay body as the service takes it, its overrides included."""
    data = changes_of(body)
    if "settings" in data:
        data["settings"] = changes_of(body.settings)
    return data


def read_settings(settings: PaySettings) -> PremiumSettings:
    """The response form of a set of settings."""
    rest = settings.rest
    return PremiumSettings(
        day_normal_minutes=settings.day_normal_minutes,
        night_normal_minutes=settings.night_normal_minutes,
        tier1_minutes=settings.tier1_minutes,
        overtime1_pct=settings.overtime1_pct,
        overtime2_pct=settings.overtime2_pct,
        rest_window=RestWindowRead(
            start_weekday=rest.start_weekday,
            start_time=rest.start_time,
            end_weekday=rest.end_weekday,
            end_time=rest.end_time,
        ),
        rest_pct=settings.rest_pct,
        increase1_points=settings.increase1_points,
        increase2_points=settings.increase2_points,
    )


def read_project(pay: ProjectPay) -> ProjectPayRead:
    """The response form of a project's pay."""
    return ProjectPayRead(
        pay_enabled=pay.pay_enabled,
        managers_see_pay=pay.managers_see_pay,
        settings=read_settings(pay.settings),
        own=pay.own,
    )


@router.get(
    "/organisation/pay-settings",
    response_model=PremiumSettings,
    summary="Read the premium defaults",
    description=(
        "Manager or Admin. The organisation's premium defaults, which every project follows unless "
        "it sets its own value: normal lengths, overtime rates, the weekly rest and its rate, and "
        "the overtime increases on a rest or special day."
    ),
)
async def read_defaults(
    current_user: User = Depends(require_manager),
    svc: PaySettingsService = Depends(pay_settings),
) -> PremiumSettings:
    """Read the premium defaults.

    Manager or Admin. The values every project follows unless it sets its own.
    """
    return read_settings(await svc.read_organisation(current_user))


@router.patch(
    "/organisation/pay-settings",
    response_model=PremiumSettings,
    summary="Change the premium defaults",
    description=(
        "Admin. Change any default; every project that has not ended and follows it uses the "
        "new value, for its earlier shifts too. Lengths are 1 to 1440 minutes, rates 100 to 300 "
        "percent and increases above 0 and at most 200 points; a weekly rest whose ends meet is "
        "refused (422 `pay.rest_window_invalid`)."
    ),
)
async def change_defaults(
    body: PremiumChanges,
    current_user: User = Depends(require_admin),
    svc: PaySettingsService = Depends(pay_settings),
) -> PremiumSettings:
    """Change the premium defaults.

    Admin. Projects that follow a default follow the change; ended projects keep their values.
    """
    return read_settings(await svc.change_organisation(current_user, changes_of(body)))


@router.get(
    "/projects/{id}/pay-settings",
    response_model=ProjectPayRead,
    summary="Read a project's pay settings",
    description=(
        "Manager or Admin. Whether the project has pay, whether its assigned Managers see it, the "
        "premium settings in effect and which of them are the project's own."
    ),
)
async def read_project_pay(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    svc: PaySettingsService = Depends(pay_settings),
) -> ProjectPayRead:
    """Read a project's pay settings.

    Manager or Admin. The switches, the settings in effect and the project's own ones.
    """
    return read_project(await svc.read_project(current_user, id))


@router.patch(
    "/projects/{id}/pay-settings",
    response_model=ProjectPayRead,
    summary="Change a project's pay settings",
    description=(
        "Admin. Switch pay on or off, open it to the project's Managers, or give a setting the "
        "project's own value (`null` returns it to the default). An ended project's settings are "
        "fixed (409 `pay.project_ended`)."
    ),
)
async def change_project_pay(
    id: uuid.UUID,
    body: ProjectPayChanges,
    current_user: User = Depends(require_admin),
    svc: PaySettingsService = Depends(pay_settings),
) -> ProjectPayRead:
    """Change a project's pay settings.

    Admin. Switches and overrides; ``null`` returns a setting to the default. 409 once ended.
    """
    return read_project(await svc.change_project(current_user, id, project_pay_changes(body)))
