#!/usr/bin/env python3
"""Parse the weekly roster workbook (a Google Sheets export) into demo data for HDS (CEO demo).

    python3 scripts/demo/parse_roster.py WORKBOOK.xlsx [--overrides demo-data/overrides.json]
        [--out demo-data/roster.json] [--since 6.9] [--report]

The workbook holds real names and pay, so everything this writes goes to the git-ignored
``demo-data/`` folder; this file holds no data of its own. Places, clients, rates and name merges
that need a person's judgement live in the overrides file, also in ``demo-data/``.

The layout it reads (one sheet per week, Sunday to Saturday): a header row with "שם משימה:"
(the task), the seven day columns to its right-to-left side with the day letters, and after the
task the security company (the client) and the task manager. Merged cells in those two columns
cover the rows under them. A cell holds names separated by commas or new lines, "?" for a place
nobody fills yet, "X" for no shift, and sometimes hours ("21:30 עד 06:30"), a sum ("900₪", "800")
or an hourly rate ("60 לשעה").
"""
from __future__ import annotations

import argparse
import datetime as dt
import difflib
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import openpyxl

DAY_LETTERS = "אבגדהוש"  # Sunday .. Saturday
TIME = r"(\d{1,2})(?::(\d{2}))?"
RANGE = re.compile(TIME + r"\s*(?:עד|-|–)\s*" + TIME)
FROM = re.compile(r"מ\s?(\d{1,2}):(\d{2})")
SUM = re.compile(r"(\d{1,2},\d{3}|\d{3,4})\s*₪?")
HOURLY = re.compile(r"(\d{2,3})\s*₪?\s*לשעה")
ROLE_WORDS = {"סיור", "הקפצה", "סייר", "אחמ\"ש", "רכב", "מאבטח"}
# Words a cell holds that are not a person: a shift's part, a rate, a bookkeeping note, a unit.
NOT_NAME_WORDS = {"בוקר", "ערב", "לילה", "החל", "חשבונית", "גבייה", "בסיס", "שכר", "משמרת", "למשמרת",
                  "לשעה", "כלבא", "כלב\"א", "רזרבה", "רגיל"}
NOT_NAMES = {"X", "x", "?", "ּ", ""}
DEFAULT_BILLING = 110  # ₪ an hour, an estimate where the sheet says nothing (marked as such)
DEFAULT_PAY = 75
TEMP_MAX_DAYS = 92


def clean(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("‏", "").replace("‎", "")).strip()


def hhmm(h: str, m: str | None) -> str:
    return f"{int(h) % 24:02d}:{int(m or 0):02d}"


def week_start(title: str, today: dt.date) -> dt.date | None:
    """The Sunday a sheet's title starts on ("15.11-21.11", "4.1.26-10.1.26", " 31.8-6.9")."""
    m = re.search(r"(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?", title)
    if not m:
        return None
    day, month = int(m.group(1)), int(m.group(2))
    years = [int(m.group(3)) % 100 + 2000] if m.group(3) else range(today.year + 1, today.year - 4, -1)
    for year in years:
        try:
            d = dt.date(year, month, day)
        except ValueError:
            continue
        if d.weekday() == 6 and d <= today + dt.timedelta(days=60):
            return d
    return None


def merged_values(ws) -> dict[tuple[int, int], object]:
    out = {}
    for rng in ws.merged_cells.ranges:
        value = ws.cell(rng.min_row, rng.min_col).value
        for r in range(rng.min_row, rng.max_row + 1):
            for c in range(rng.min_col, rng.max_col + 1):
                out[(r, c)] = value
    return out


def task_windows(text: str) -> list[tuple[str, str]]:
    """The time windows a task's description names ("23:00-5", "21 עד 06", "16/19 עד 07")."""
    text = re.sub(r"(\d{1,2})/(\d{1,2})(\s*עד)", r"\2\3", text)  # "16/19 עד 07": the later start
    text = re.sub(r"\(\s*\d+\s*-\s*\d+[^)]*\)", "", text)  # "(1-2 סיירים)" is a headcount
    wins = []
    for m in RANGE.finditer(text):
        a, b = int(m.group(1)), int(m.group(3))
        if a > 24 or b > 24:
            continue
        wins.append((hhmm(m.group(1), m.group(2)), hhmm(m.group(3), m.group(4))))
    return wins


def task_rates(text: str) -> dict:
    """Billing and pay a task's description states; per hour unless it says a shift's sum."""
    t = re.sub(r"\d{1,2}/\d{1,2}\s*עד", " ", text)  # "16/19 עד 07" is a start, not a rate
    t = RANGE.sub(" ", t).replace(",", " ").replace("₪", " ")
    out: dict = {}
    m = re.search(r"גבייה\s*(\d+)\s*שכר\s*(\d+)", t)
    if m:
        bill, pay = int(m.group(1)), int(m.group(2))
        unit = "shift" if max(bill, pay) >= 300 else "hour"
        return {"billing": bill, "pay": pay, "billing_unit": unit, "pay_unit": unit}
    m = re.search(r"גבייה\s*(\d+)\s*לשעה", t)
    if m:
        out["billing"], out["billing_unit"] = int(m.group(1)), "hour"
    m = re.search(r"שכר\s*(\d+)\s*(למשמרת)?", t)
    if m:
        pay = int(m.group(1))
        out["pay"], out["pay_unit"] = pay, ("shift" if m.group(2) or pay >= 300 else "hour")
    m = re.search(r"(?:גבייה\s*)?(\d{2,3})\s*/\s*(\d{2,3})", t)
    if m and "billing" not in out and min(int(m.group(1)), int(m.group(2))) >= 30:
        a, b = int(m.group(1)), int(m.group(2))
        out.update(billing=max(a, b), pay=min(a, b), billing_unit="hour", pay_unit="hour")
    m = re.search(r"(?:^|\s)ג\s*(\d{2,3})\s*₪", t)
    if m and "billing" not in out:
        out["billing"], out["billing_unit"] = int(m.group(1)), "hour"
    m = HOURLY.search(t)
    if m and not out:
        v = int(m.group(1))
        key = "billing" if v >= 100 else "pay"
        out[key], out[f"{key}_unit"] = v, "hour"
    return out


def task_key(text: str) -> str:
    """A task's identity across weeks: its first words without hours, rates or punctuation."""
    first = clean(text.split("\n")[0])
    first = re.sub(r"\(.*?\)", " ", first)
    first = RANGE.sub(" ", first)
    first = re.sub(r"(?<!\S)(גבייה|שכר|לשעה|למשמרת|עד|משמרת|בוקר|ערב|לילה|שעות משתנות|לבדוק.*$)(?!\S)",
                   " ", first)
    first = re.sub(r"[\d₪*/:,.\-–]+", " ", first)
    return clean(first)


def parse_entry(raw: str) -> dict | None:
    """One person in a cell: their name and what the cell adds (hours, a sum, a rate, a note)."""
    text = clean(raw)
    if text in NOT_NAMES:
        return None
    entry: dict = {}
    m = RANGE.search(text)
    if m and int(m.group(1)) <= 24 and int(m.group(3)) <= 24:
        entry["start"], entry["end"] = hhmm(m.group(1), m.group(2)), hhmm(m.group(3), m.group(4))
        text = text[: m.start()] + " " + text[m.end():]
    m = FROM.search(text)
    if m:
        entry["start"] = hhmm(m.group(1), m.group(2))
        text = text[: m.start()] + " " + text[m.end():]
    m = HOURLY.search(text)
    if m:
        entry["hourly"] = int(m.group(1))
        text = text[: m.start()] + " " + text[m.end():]
    m = SUM.search(text)
    if m:
        value = int(m.group(1).replace(",", ""))
        if value >= 300:
            entry["sum"] = value
            text = text[: m.start()] + " " + text[m.end():]
    parts = [clean(p) for p in re.split(r"\s+-\s+|\s-|-\s", text) if clean(p)]
    name = ""
    notes = []
    for p in parts:
        if not name and p not in ROLE_WORDS and not re.search(r"\d", p) and "לבדוק" not in p:
            name = p
        else:
            notes.append(p)
    name = clean(re.sub(r"[^֐-׿A-Za-z'\" ]", " ", name))
    words = [w for w in name.split(" ") if w and w not in NOT_NAME_WORDS]
    while len(words) > 1 and len(words[-1]) == 1:  # "דנה לוי מ": the "מ" of "מ18:00"
        words.pop()
    name = " ".join(words)
    if len(name) < 2 or name in ROLE_WORDS:
        return None
    entry["name"] = name
    if notes:
        entry["note"] = " · ".join(notes)
    return entry


def split_cell(value) -> tuple[list[dict], int]:
    """A day cell: the people on it and how many places are still open ("?")."""
    if value is None:
        return [], 0
    text = str(value).replace("‏", "")
    if clean(text) in ("X", "x", "", "ּ"):
        return [], 0
    pieces = [p for p in re.split(r"[,\n]|\s{3,}", text) if clean(p)]
    people, open_places = [], 0
    for p in pieces:
        p = clean(p)
        q = p.count("?")
        if q and clean(p.replace("?", "")) == "":
            open_places += q
            continue
        e = parse_entry(p.replace("?", ""))
        if e:
            people.append(e)
        open_places += q
    return people, open_places


def read_week(ws, start: dt.date) -> dict | None:
    header = None
    for row in ws.iter_rows(min_row=1, max_row=10):
        for c in row:
            if isinstance(c.value, str) and "שם משימה" in c.value:
                header = (c.row, c.column)
    if not header:
        return None
    r0, tc = header
    merged = merged_values(ws)

    def val(r, c):
        v = ws.cell(r, c).value
        return merged.get((r, c), v) if v is None else v

    day_cols = {}
    for c in range(max(1, tc - 7), tc):
        letter = clean(str(ws.cell(r0, c).value or ""))[:1]
        if letter in DAY_LETTERS:
            day_cols[c] = start + dt.timedelta(days=DAY_LETTERS.index(letter))
    rows, needs = [], []
    current = None
    last_client = last_manager = None
    for r in range(r0 + 1, ws.max_row + 1):
        task_cell = ws.cell(r, tc).value
        task_text = merged.get((r, tc), task_cell) if task_cell is None else task_cell
        cells = {c: ws.cell(r, c).value for c in day_cols}
        has_days = any(v not in (None, "") for v in cells.values())
        client = val(r, tc + 1)
        manager = val(r, tc + 2)
        if isinstance(task_text, str) and clean(task_text) and clean(task_text) != "X":
            text = task_text.strip()
            if "כמה צריך" in text:
                needs.append({"about": clean(text.replace("כמה צריך", "")).lstrip("ב"),
                              "days": {str(day_cols[c]): int(float(v)) for c, v in cells.items()
                                       if isinstance(v, (int, float)) or (isinstance(v, str) and re.fullmatch(r"\d+(\.0)?", v.strip()))}})
                continue
            if "רזרבה" in text or not re.search(r"[א-תA-Za-z]", text):
                current = None
                continue
            same_task = task_cell is None and current is not None and current["text"] == text
            if not same_task:
                if isinstance(client, str) and clean(client):
                    last_client = clean(client)
                if isinstance(manager, str) and clean(manager):
                    last_manager = clean(manager)
                current = {"text": text, "client": last_client, "manager": last_manager, "rows": []}
                rows.append(current)
        elif not has_days:
            continue
        if current is None or not has_days:
            continue
        line = {}
        for c, day in day_cols.items():
            people, open_places = split_cell(cells[c])
            if people or open_places:
                line[str(day)] = {"people": people, "open": open_places}
        current["rows"].append(line)
    return {"start": str(start), "title": ws.title.strip(), "tasks": rows, "needs": needs}


def all_text(ws) -> str:
    return " ".join(str(c.value) for row in ws.iter_rows() for c in row if isinstance(c.value, str))


def _hours(window) -> float:
    a, b = (int(x[:2]) + int(x[3:]) / 60 for x in window)
    return (b - a) % 24 or 24


def three_months_on(day: dt.date) -> dt.date:
    """``day`` three calendar months on, on the month's last day when it is shorter."""
    import calendar

    month = day.month + 2
    year, month = day.year + month // 12, month % 12 + 1
    return dt.date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def _sunday(day: str) -> str:
    d = dt.date.fromisoformat(day)
    return str(d - dt.timedelta(days=d.isoweekday() % 7))


def similar(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, a, b).ratio()


def merge_names(counts: Counter, overrides: dict) -> dict[str, str]:
    """Each spelling to one name: the overrides first, then near-identical spellings (a typo,
    a letter swapped) to the commoner one, then a lone first or last name that only one full
    name carries."""
    canon = dict(overrides.get("names", {}))
    names = sorted(counts, key=lambda n: -counts[n])
    for n in names:
        if n in canon:
            continue
        for m in names:
            if m == n or counts[m] < counts[n] or m in canon and canon[m] != m:
                continue
            if " " in n and " " in m and similar(n, m) >= 0.86:
                canon[n] = canon.get(m, m)
                break
    full = [n for n in names if " " in n and canon.get(n, n) == n]
    for n in names:
        if n in canon or " " in n:
            continue
        hits = {f for f in full if n in f.split(" ")}
        if len(hits) == 1:
            canon[n] = hits.pop()
    return {n: canon.get(n, n) for n in names}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("workbook")
    ap.add_argument("--overrides", default="demo-data/overrides.json")
    ap.add_argument("--out", default="demo-data/roster.json")
    ap.add_argument("--since", default="6.9", help="the first week's sheet title (its layout)")
    ap.add_argument("--today", default=str(dt.date.today()))
    ap.add_argument("--report", action="store_true", help="print what needs a person's look")
    args = ap.parse_args()
    today = dt.date.fromisoformat(args.today)
    overrides = json.loads(Path(args.overrides).read_text()) if Path(args.overrides).exists() else {}

    wb = openpyxl.load_workbook(args.workbook, data_only=True)
    titles = [t for t in wb.sheetnames]
    first = next(i for i, t in enumerate(titles) if t.strip().startswith(args.since + "-"))
    weeks = []
    for t in titles[first:]:
        start = week_start(t, today)
        if start is None:
            continue
        w = read_week(wb[t], start)
        if w:
            weeks.append(w)
    weeks.sort(key=lambda w: w["start"])
    history = []  # every sheet's start and text, for when someone first appears
    for t in titles:
        start = week_start(t, today)
        if start:
            history.append((start, all_text(wb[t])))

    # Tasks become projects.
    task_alias = overrides.get("tasks", {})
    projects: dict[str, dict] = {}
    for w in weeks:
        for task in w["tasks"]:
            key = task_alias.get(task_key(task["text"]), {}).get("key") or task_key(task["text"])
            p = projects.setdefault(key, {"key": key, "texts": Counter(), "clients": Counter(),
                                          "managers": Counter(), "windows": [], "rates": {}})
            p["texts"][clean(task["text"])] += 1
            if task["client"]:
                p["clients"][task["client"]] += 1
            if task["manager"]:
                p["managers"][task["manager"]] += 1
            wins = task_windows(task["text"])
            if any(_hours(w) >= 3 for w in wins):  # "<street> 2-4" is an address, not 02:00-04:00
                wins = [w for w in wins if _hours(w) >= 3]
            if len(wins) > len(p["windows"]):
                p["windows"] = wins
            p["rates"].update(task_rates(task["text"]))
            task["key"] = key

    # People.
    counts: Counter = Counter()
    for w in weeks:
        for task in w["tasks"]:
            for line in task["rows"]:
                for cell in line.values():
                    for e in cell["people"]:
                        counts[e["name"]] += 1
    drop = set(overrides.get("not_people", []))
    canon = merge_names(Counter({n: c for n, c in counts.items() if n not in drop}), overrides)

    shifts = []
    seen = defaultdict(lambda: {"dates": set(), "projects": Counter()})
    for w in weeks:
        for task in w["tasks"]:
            proj = projects[task["key"]]
            for i, line in enumerate(task["rows"]):
                wins = proj["windows"] or [tuple(task_alias.get(task["key"], {}).get("window", ["20:00", "06:00"]))]
                win = wins[min(i, len(wins) - 1)]
                for day, cell in line.items():
                    people = [dict(e, name=canon[e["name"]]) for e in cell["people"] if e["name"] in canon]
                    for e in people:
                        seen[e["name"]]["dates"].add(day)
                        seen[e["name"]]["projects"][task["key"]] += 1
                    shifts.append({"project": task["key"], "date": day, "row": i,
                                   "start": win[0], "end": win[1],
                                   "headcount": len(people) + cell["open"],
                                   "open": cell["open"], "people": people})

    # Needs from the "how many are needed" rows; otherwise the usual headcount by weekday.
    for w in weeks:
        for need in w["needs"]:
            match = [k for k, p in projects.items() if need["about"] and need["about"] in k]
            for k in match[:1]:
                for day, n in need["days"].items():
                    projects[k].setdefault("needs_by_date", {})[day] = n

    out_projects = []
    for k, p in projects.items():
        if not any(s["project"] == k and s["people"] for s in shifts):
            continue  # a task nobody worked in these weeks
        o = task_alias.get(k, {})
        rates = dict(p["rates"])
        rates.update(o.get("rates", {}))
        estimated = "billing" not in rates or "pay" not in rates
        rates.setdefault("billing", DEFAULT_BILLING)
        rates.setdefault("billing_unit", "hour")
        rates.setdefault("pay", DEFAULT_PAY)
        rates.setdefault("pay_unit", "hour")
        by_weekday = defaultdict(list)
        for s in shifts:
            if s["project"] == k and s["row"] == 0:
                by_weekday[dt.date.fromisoformat(s["date"]).isoweekday() % 7].append(s["headcount"])
        out_projects.append({
            "key": k,
            "name": o.get("name", k),
            "description": p["texts"].most_common(1)[0][0],
            "client": o.get("client") or (p["clients"].most_common(1)[0][0] if p["clients"] else None),
            "manager": o.get("manager") or (p["managers"].most_common(1)[0][0] if p["managers"] else None),
            "site": o.get("site", k),
            "type": o.get("type", "training" if "עיון" in k or "הדרכ" in k or "קורס" in k else "security"),
            "windows": [list(x) for x in (p["windows"] or [o.get("window", ["20:00", "06:00"])])],
            "rates": rates,
            "rates_estimated": o.get("rates_estimated", estimated),
            "needs_by_weekday": {str(d): max(Counter(v).most_common(1)[0][0], 1) for d, v in by_weekday.items()},
            "needs_by_date": p.get("needs_by_date", {}),
            "weeks": len({_sunday(s["date"]) for s in shifts if s["project"] == k}),
        })

    # Temporary workers: seen for under 3 months in the whole workbook, on one or two projects.
    people_out = []
    for name, info in sorted(seen.items(), key=lambda x: -len(x[1]["dates"])):
        dates = sorted(info["dates"])
        first_seen = dt.date.fromisoformat(dates[0])
        for start, text in sorted(history):
            if name in text:
                first_seen = min(first_seen, start)
                break
        last_seen = dt.date.fromisoformat(dates[-1])
        span = (last_seen - first_seen).days
        # New in the last twelve weeks, still working in the last three, on one or two projects.
        temp = (span < TEMP_MAX_DAYS and len(info["projects"]) <= 2
                and first_seen >= today - dt.timedelta(days=84) and last_seen >= today - dt.timedelta(days=21))
        temp = overrides.get("temporary", {}).get(name, temp)
        person = {"name": name, "aliases": sorted(a for a, c in canon.items() if c == name and a != name),
                  "first_seen": str(first_seen), "last_seen": str(last_seen), "shifts": len(dates),
                  "projects": [k for k, _ in info["projects"].most_common()], "temporary": temp}
        if temp:
            person["engaged_from"] = str(first_seen)
            # Three calendar months from their first week, the longest a temporary engagement runs.
            person["engaged_until"] = str(three_months_on(first_seen) - dt.timedelta(days=1))
            person["projects"] = person["projects"][:2]
        people_out.append(person)

    clients = sorted({p["client"] for p in out_projects if p["client"]})
    managers = sorted({p["manager"] for p in out_projects if p["manager"]})
    data = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "source": Path(args.workbook).name,
        "today": str(today),
        "weeks": [w["start"] for w in weeks],
        "clients": clients,
        "managers": managers,
        "projects": out_projects,
        "people": people_out,
        "shifts": shifts,
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(data, ensure_ascii=False, indent=1))
    print(f"{len(weeks)} weeks {data['weeks'][0]}..{data['weeks'][-1]} · {len(out_projects)} projects · "
          f"{len(clients)} clients · {len(managers)} managers · {len(people_out)} people "
          f"({sum(p['temporary'] for p in people_out)} temporary) · {len(shifts)} shifts · "
          f"{sum(s['open'] for s in shifts)} open places → {args.out}")
    if args.report:
        print("\nPROJECTS")
        for p in sorted(out_projects, key=lambda p: -p["weeks"]):
            print(f"  {p['key']!r:42} weeks={p['weeks']:2} client={p['client']} mgr={p['manager']} "
                  f"win={p['windows']} rates={p['rates']} est={p['rates_estimated']} type={p['type']}")
        print("\nMERGED NAMES")
        for a, c in sorted(canon.items()):
            if a != c:
                print(f"  {a} → {c}")
        print("\nSINGLE-WORD NAMES (check)")
        print("  " + ", ".join(sorted(p["name"] for p in people_out if " " not in p["name"])))
        print("\nNEAR PAIRS NOT MERGED (check)")
        names = [p["name"] for p in people_out]
        for i, a in enumerate(names):
            for b in names[i + 1:]:
                if 0.75 <= similar(a, b) < 0.86:
                    print(f"  {a} ~ {b}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
