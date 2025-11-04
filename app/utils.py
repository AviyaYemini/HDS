import json
from datetime import datetime, timedelta, date
from typing import Any, Dict, Iterable, Optional, Set


def calculate_shift_hours(date_str: str, start_time: str, end_time: str) -> float:
    """Compute hours between start and end times for a given date."""
    try:
        start_dt = datetime.strptime(f"{date_str} {start_time}", "%Y-%m-%d %H:%M")
        end_dt = datetime.strptime(f"{date_str} {end_time}", "%Y-%m-%d %H:%M")
    except ValueError:
        return 0.0

    if end_dt <= start_dt:
        end_dt += timedelta(days=1)

    duration = end_dt - start_dt
    return round(duration.total_seconds() / 3600, 2)


_SHIFT_ALIASES = {
    "morning": "morning",
    "בוקר": "morning",
    "afternoon": "afternoon",
    "noon": "afternoon",
    "צהריים": "afternoon",
    "evening": "afternoon",
    "night": "night",
    "overnight": "night",
    "לילה": "night",
}


def normalize_shift_key(value: str) -> str:
    if not value:
        return value
    lowered = value.strip().lower()
    return _SHIFT_ALIASES.get(lowered, lowered)


def _parse_date(value: str) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


def _ensure_set(container: Optional[Set[str]], items: Iterable[str]) -> Set[str]:
    base = container or set()
    for item in items:
        if item:
            base.add(normalize_shift_key(str(item)))
    return base


def build_constraint_profile(constraints_rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build a normalized constraint profile for an employee.
    Supported patterns (value_json dictionaries or lists):
        - allowed/blocked shift keys
        - allowed/blocked dates (YYYY-MM-DD)
    """
    profile: Dict[str, Any] = {
        "allowed_shifts": None,  # type: Optional[Set[str]]
        "blocked_shifts": set(),  # type: Set[str]
        "preferred_shifts": set(),  # type: Set[str]
        "disliked_shifts": set(),  # type: Set[str]
        "required_shifts": None,  # type: Optional[Set[str]]
        "allowed_dates": None,  # type: Optional[Set[str]]
        "blocked_dates": set(),  # type: Set[str]
    }

    for row in constraints_rows:
        value_json = row.get("value_json")
        try:
            parsed = json.loads(value_json) if value_json else None
        except json.JSONDecodeError:
            parsed = value_json

        kind = (row.get("kind") or "").lower()
        scope = (row.get("scope") or "").lower()

        def _apply_shift_values(values: Iterable[str], action: str, priority: str = ""):
            normalized_values = [normalize_shift_key(str(v)) for v in values if v]
            if not normalized_values:
                return
            if action == "allow":
                profile["allowed_shifts"] = _ensure_set(profile["allowed_shifts"], normalized_values)
            elif action == "block":
                profile["blocked_shifts"] = _ensure_set(profile["blocked_shifts"], normalized_values)

            if priority in ("required", "must"):
                profile["required_shifts"] = _ensure_set(profile.get("required_shifts"), normalized_values)
            elif priority in ("preferred", "like", "pref"):
                profile["preferred_shifts"] = _ensure_set(profile.get("preferred_shifts"), normalized_values)
            elif priority in ("avoid", "dislike"):
                profile["disliked_shifts"] = _ensure_set(profile.get("disliked_shifts"), normalized_values)

        def _maybe_apply_dates(values: Iterable[str], allow: bool):
            normalized = []
            for item in values:
                if item:
                    normalized.append(str(item))
            if allow:
                existing = profile["allowed_dates"] or set()
                existing.update(normalized)
                profile["allowed_dates"] = existing
            else:
                profile["blocked_dates"].update(normalized)

        priority_hint = (row.get("priority") or "").lower()

        if isinstance(parsed, dict):
            priority = str(parsed.get("priority") or priority_hint or row.get("kind") or "").lower()
            for key, value in parsed.items():
                lowered_key = key.lower()
                if "shift" in lowered_key and isinstance(value, list):
                    if "allow" in lowered_key or "מותר" in lowered_key:
                        _apply_shift_values(value, "allow", priority)
                    elif "block" in lowered_key or "not" in lowered_key or "אסור" in lowered_key:
                        _apply_shift_values(value, "block", priority)
                elif "allowed" in lowered_key and isinstance(value, list):
                    if "date" in lowered_key:
                        _maybe_apply_dates(value, allow=True)
                    else:
                        _apply_shift_values(value, "allow", priority)
                elif "blocked" in lowered_key and isinstance(value, list):
                    if "date" in lowered_key:
                        _maybe_apply_dates(value, allow=False)
                    else:
                        _apply_shift_values(value, "block", priority)
                elif lowered_key in ("values", "shifts") and isinstance(value, list):
                    action = parsed.get("action") or parsed.get("type") or row.get("scope") or "allow"
                    action_lower = str(action).lower()
                    if "block" in action_lower or "אסור" in action_lower:
                        _apply_shift_values(value, "block", priority)
                    else:
                        _apply_shift_values(value, "allow", priority)
        elif isinstance(parsed, list):
            if "shift" in scope:
                allow = not any(token in kind for token in ("un", "לא", "אסור", "block"))
                action = "allow" if allow else "block"
                _apply_shift_values(parsed, action, priority_hint or kind)
            elif "date" in scope:
                allow = not any(token in kind for token in ("un", "לא", "אסור", "block"))
                _maybe_apply_dates(parsed, allow=allow)
        elif isinstance(parsed, str):
            if "shift" in scope:
                allow = not any(token in kind for token in ("un", "לא", "אסור", "block"))
                action = "allow" if allow else "block"
                _apply_shift_values([parsed], action, priority_hint or kind)
            elif "date" in scope:
                allow = not any(token in kind for token in ("un", "לא", "אסור", "block"))
                _maybe_apply_dates([parsed], allow=allow)

    return profile


def constraint_allows_shift(
    profile: Dict[str, Any],
    date_str: str,
    shift_key: str,
) -> bool:
    """Check if the normalized profile allows working the shift on the given date."""
    normalized_shift = normalize_shift_key(shift_key)
    shift_date = _parse_date(date_str)

    allowed_dates: Optional[Set[str]] = profile.get("allowed_dates")
    blocked_dates: Set[str] = profile.get("blocked_dates", set())
    if allowed_dates and date_str not in allowed_dates:
        return False
    if date_str in blocked_dates:
        return False

    allowed_shifts: Optional[Set[str]] = profile.get("allowed_shifts")
    blocked_shifts: Set[str] = profile.get("blocked_shifts", set())
    if allowed_shifts and normalized_shift not in allowed_shifts:
        return False
    if normalized_shift in blocked_shifts:
        return False

    required_shifts: Optional[Set[str]] = profile.get("required_shifts")
    if required_shifts and normalized_shift not in required_shifts:
        return False

    return True
