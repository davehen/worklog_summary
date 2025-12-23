#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional, Tuple

import requests


ZONE = ZoneInfo("Europe/Luxembourg")
HOURS_PER_WORKDAY = 8


def die(msg: str, code: int = 2) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def fmt_hms(seconds: int) -> str:
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h}:{m:02d}:{s:02d}"


def fmt_pct(value: float) -> str:
    return f"{value:.2f}%"


def count_business_days(start_inclusive: date, end_exclusive: date) -> int:
    d = start_inclusive
    cnt = 0
    while d < end_exclusive:
        if d.weekday() < 5:
            cnt += 1
        d += timedelta(days=1)
    return cnt


def resolve_month_bounds(month: Optional[str]) -> Tuple[str, datetime, datetime, date, date]:
    if month and month.strip():
        try:
            year, mon = map(int, month.split("-"))
            if not (1 <= mon <= 12):
                raise ValueError
        except Exception:
            die("--month must be in YYYY-MM format")
    else:
        now = datetime.now(ZONE)
        year, mon = now.year, now.month
        month = f"{year:04d}-{mon:02d}"

    start_date = date(year, mon, 1)
    end_excl_date = date(year + (mon == 12), 1 if mon == 12 else mon + 1, 1)

    start = datetime.combine(start_date, time.min, tzinfo=ZONE)
    end_excl = datetime.combine(end_excl_date, time.min, tzinfo=ZONE)

    return month, start, end_excl, start_date, end_excl_date


def parse_jira_started(s: str) -> datetime:
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    die(f"Cannot parse Jira worklog timestamp: {s}")


@dataclass
class JiraClient:
    base_url: str
    email: str
    api_token: str
    session: requests.Session

    @classmethod
    def create(cls, base_url: str, email: str, api_token: str) -> "JiraClient":
        s = requests.Session()
        s.auth = (email, api_token)
        s.headers.update({"Accept": "application/json"})
        return cls(base_url.rstrip("/"), email, api_token, s)

    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        r = self.session.get(f"{self.base_url}{path}", params=params, timeout=60)
        if r.status_code != 200:
            die(f"HTTP {r.status_code} calling {path}: {r.text}")
        return r.json()

    def myself(self) -> Dict[str, Any]:
        return self.get_json("/rest/api/3/myself")

    def search_issues(self, jql: str) -> List[Dict[str, Any]]:
        issues = []
        start_at = 0
        while True:
            body = self.get_json(
                "/rest/api/3/search/jql",
                {
                    "jql": jql,
                    "startAt": start_at,
                    "maxResults": 100,
                    "fields": "key,labels,summary",
                },
            )
            page = body.get("issues", [])
            issues.extend(page)
            start_at += len(page)
            if start_at >= body.get("total", 0):
                break
        return issues

    def worklogs(self, issue_key: str) -> List[Dict[str, Any]]:
        logs = []
        start_at = 0
        while True:
            body = self.get_json(
                f"/rest/api/3/issue/{issue_key}/worklog",
                {"startAt": start_at, "maxResults": 100},
            )
            page = body.get("worklogs", [])
            logs.extend(page)
            start_at += len(page)
            if start_at >= body.get("total", 0):
                break
        return logs


def normalize_prefixes(args: List[str]) -> List[str]:
    out = []
    for a in args:
        out.extend(p.strip() for p in a.split(",") if p.strip())
    return list(dict.fromkeys(out))


def issue_matches(labels: List[str], prefixes: List[str]) -> bool:
    return any(lab.startswith(p) for lab in labels for p in prefixes)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", required=True, help="Jira base URL (https://xxx.atlassian.net)")
    p.add_argument("--email", required=True, help="Jira account email")
    p.add_argument("--api-token", required=True, help="Atlassian API token")
    p.add_argument("--label-prefix", required=True, action="append", help="Repeatable or comma-separated")
    p.add_argument("--month", help="YYYY-MM (default: current month)")

    a = p.parse_args()

    prefixes = normalize_prefixes(a.label_prefix)
    ym, start, end_excl, start_d, end_d = resolve_month_bounds(a.month)

    business_days = count_business_days(start_d, end_d)
    capacity_seconds = business_days * HOURS_PER_WORKDAY * 3600

    jira = JiraClient.create(a.base_url, a.email, a.api_token)
    me = jira.myself()
    account_id = me["accountId"]
    display_name = me["displayName"]

    jql = f"""
worklogAuthor = currentUser()
AND worklogDate >= "{start_d}"
AND worklogDate <= "{(end_d - timedelta(days=1))}"
ORDER BY updated DESC
""".strip()

    total = 0
    per_issue = []

    for issue in jira.search_issues(jql):
        labels = issue["fields"].get("labels", [])
        if not issue_matches(labels, prefixes):
            continue

        seconds = 0
        for wl in jira.worklogs(issue["key"]):
            if wl["author"]["accountId"] != account_id:
                continue
            started = parse_jira_started(wl["started"]).astimezone(ZONE)
            if start <= started < end_excl:
                seconds += wl.get("timeSpentSeconds", 0)

        if seconds:
            total += seconds
            per_issue.append(
                {"key": issue["key"], "summary": issue["fields"]["summary"], "seconds": seconds}
            )

    per_issue.sort(key=lambda r: r["seconds"], reverse=True)

    pct = (100 * total / capacity_seconds) if capacity_seconds else 0

    print(f"User: {display_name}")
    print(f"Month ({ZONE.key}): {start_d} to {end_d - timedelta(days=1)}  [{ym}]")
    print(f"Label prefixes: {', '.join(prefixes)}*")
    print()
    print(
        f"Business days in month: {business_days}  | Assumed hours/day: {HOURS_PER_WORKDAY}  | Capacity: {fmt_hms(capacity_seconds)}"
    )
    print(
        f"TOTAL on matching tickets: {fmt_hms(total)} ({total} seconds)  | % of month capacity: {fmt_pct(pct)}"
    )
    print()
    print(f"{'#':<3} {'Issue':<15} {'Time':>10} {'Cumulated':>12} | Summary")

    running = 0
    for i, r in enumerate(per_issue, 1):
        running += r["seconds"]
        print(
            f"{i:<3} {r['key']:<15} {fmt_hms(r['seconds']):>10} {fmt_hms(running):>12} | {r['summary']}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
