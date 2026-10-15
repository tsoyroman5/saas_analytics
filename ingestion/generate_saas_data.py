#!/usr/bin/env python3
"""
Synthetic B2B SaaS data generator for the analytics-engineering portfolio project.

SYNTHETIC DATA. Nothing here describes a real company. Every number comes from the
parameters below and a fixed random seed, so the same seed gives byte-identical files.

What it writes (all under --out):

  data/batch_date=YYYY-MM-DD/
      subscription_events.csv   append-only log of subscription state changes
      invoices.csv              one row per invoice issued that day
      payment_attempts.csv      one row per payment attempt that day (failures and retries)
      product_events.csv        usage events ARRIVING that day (some are late, some duplicated)
      customers.csv             full dump of the customers table at end of that day
  seeds/plan_prices.csv         price per seat per plan and billing interval (a dbt seed)
  truth/                        ground truth used to reconcile the dbt models
      truth_customer_month_mrr.csv
      truth_mrr_monthly.csv
      truth_cohort_retention.csv
      truth_planted_issues.json

Planted data problems (so the tests have something to catch):
  - late-arriving product events (event_ts is days before the batch that delivers them)
  - duplicated product events and duplicated subscription events (same id, later batch)
  - orphan product events (customer_id that does not exist in customers)
  - failed payments, retries, and involuntary churn after repeated failures

Metric definitions the truth files follow (see DESIGN.md):
  - MRR of a customer at a month end = seats * price_per_seat(plan, billing_interval),
    0 if the subscription is canceled at the end of that month.
  - Annual plans use the discounted per-seat price, so MRR is the annual price / 12.
  - The MRR bridge is computed from month-end snapshots only, so changes that start and
    end inside one month net out (a customer who signs up and cancels in the same month
    is never counted).
"""
from __future__ import annotations

import argparse
import calendar
import csv
import json
import math
import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

# ----------------------------------------------------------------------------------
# Parameters. Change these, not the code, to change the world.
# ----------------------------------------------------------------------------------
PLANS = ["starter", "pro", "business"]
# Price per seat per month, in cents. Annual billing gets a 20% discount.
MONTHLY_PRICE = {"starter": 1200, "pro": 2500, "business": 4500}
ANNUAL_DISCOUNT = 0.20
PRICE = {
    (p, "monthly"): MONTHLY_PRICE[p] for p in PLANS
} | {
    (p, "annual"): round(MONTHLY_PRICE[p] * (1 - ANNUAL_DISCOUNT)) for p in PLANS
}

PLAN_MIX = {"starter": 0.55, "pro": 0.33, "business": 0.12}
SEATS_RANGE = {"starter": (1, 5), "pro": (3, 15), "business": (10, 60)}
ANNUAL_SHARE = {"starter": 0.10, "pro": 0.25, "business": 0.45}

# Voluntary churn. Monthly customers: monthly hazard, higher in the first 90 days.
MONTHLY_CHURN = {"starter": 0.045, "pro": 0.028, "business": 0.015}
EARLY_CHURN_MULT = 1.8
# Annual customers decide at renewal.
ANNUAL_RENEWAL_CHURN = {"starter": 0.25, "pro": 0.15, "business": 0.10}

SEAT_CHANGE_PROB_MONTHLY = 0.10     # per active customer per month
UPGRADE_PROB_MONTHLY = 0.015
DOWNGRADE_PROB_MONTHLY = 0.005
REACTIVATION_PROB_DAILY = 0.0015    # applies from 30 to 365 days after a voluntary cancel

SIGNUPS_PER_DAY_START = 1.5
SIGNUPS_PER_DAY_GROWTH = 0.004      # added per day since start

PAYMENT_FAIL_PROB = {"starter": 0.06, "pro": 0.04, "business": 0.02}
RETRY_DELAYS = [3, 7]               # days after the previous failed attempt
RETRY_SUCCESS_PROB = [0.60, 0.50]

# Product usage.
ACTIVE_DAY_PROB_WEEKDAY = 0.55
ACTIVE_DAY_PROB_WEEKEND = 0.08
EVENTS_PER_ACTIVE_DAY = 3
EVENT_NAMES = ["login", "dashboard_viewed", "report_created", "export", "invite_sent"]
EVENT_WEIGHTS = [0.35, 0.30, 0.15, 0.12, 0.08]

# Planted problems.
LATE_EVENT_PROB = 0.03              # arrives 1 to 5 days after event_ts
DUP_PRODUCT_EVENT_PROB = 0.005
DUP_SUB_EVENT_PROB = 0.003
ORPHAN_EVENT_PROB = 0.001

COUNTRIES = ["US", "GB", "DE", "FR", "NL", "PL", "SE", "CA", "AU", "KR", "SG", "BR"]
COUNTRY_W = [0.28, 0.10, 0.10, 0.07, 0.06, 0.05, 0.04, 0.08, 0.05, 0.04, 0.05, 0.08]
NAME_A = ["Blue", "North", "Bright", "Atlas", "Nova", "Pixel", "Cedar", "Quartz", "Lumen", "Orbit",
          "Maple", "Delta", "Summit", "Harbor", "Ion", "Vector", "Willow", "Zenith"]
NAME_B = ["Labs", "Works", "Systems", "Group", "Analytics", "Logistics", "Media", "Health",
          "Foods", "Studio", "Cloud", "Partners", "Robotics", "Retail", "Energy"]


def poisson(rng: random.Random, lam: float) -> int:
    if lam <= 0:
        return 0
    if lam > 30:  # normal approximation, not needed at our rates but safe
        return max(0, int(round(rng.gauss(lam, math.sqrt(lam)))))
    limit, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


def add_months(d: date, months: int, anchor_day: int) -> date:
    y, m = divmod(d.year * 12 + d.month - 1 + months, 12)
    m += 1
    return date(y, m, min(anchor_day, calendar.monthrange(y, m)[1]))


def month_end(d: date) -> bool:
    return (d + timedelta(days=1)).month != d.month


@dataclass
class Customer:
    customer_id: str
    company_name: str
    country: str
    signup_date: date
    plan: str
    seats: int
    interval: str
    anchor_day: int
    active: bool = True
    next_invoice: date | None = None
    cancel_date: date | None = None
    cancel_reason: str | None = None
    # payment retry state
    open_invoice: dict | None = None
    retry_idx: int = 0
    next_retry: date | None = None
    started_at: date | None = None   # start of the current subscription period
    seats_at_cancel: int = 0
    ev_day: date | None = None       # day of the last subscription event (for ordered timestamps)
    ev_n: int = 0

    def mrr(self) -> int:
        return self.seats * PRICE[(self.plan, self.interval)] if self.active else 0


class Generator:
    def __init__(self, start: date, end: date, seed: int):
        self.start, self.end = start, end
        self.rng = random.Random(seed)
        self.customers: dict[str, Customer] = {}
        self.order: list[str] = []
        self.n_customers = 0
        self.n_events = 0
        self.n_invoices = 0
        self.n_attempts = 0
        self.n_prod = 0
        self.batches: dict[date, dict[str, list]] = defaultdict(
            lambda: {"sub": [], "inv": [], "pay": [], "prod": []}
        )
        self.month_mrr: dict[str, dict[str, int]] = {}      # 'YYYY-MM' -> {cust: mrr}
        self.first_start: dict[str, date] = {}
        self.issues = defaultdict(int)

    # -- helpers ---------------------------------------------------------------
    def ts(self, d: date) -> datetime:
        return datetime(d.year, d.month, d.day) + timedelta(seconds=self.rng.randint(0, 86399))

    def emit_sub_event(self, c: Customer, d: date, etype: str, reason: str | None = None):
        self.n_events += 1
        # Timestamps must increase with the order the events happened in, per customer.
        if c.ev_day != d:
            c.ev_day, c.ev_n = d, 0
        ts = datetime(d.year, d.month, d.day) + timedelta(seconds=3600 * c.ev_n + self.rng.randint(0, 3599))
        c.ev_n += 1
        row = {
            "event_id": f"se_{self.n_events:08d}",
            "customer_id": c.customer_id,
            "event_type": etype,
            "event_ts": ts.isoformat(sep=" "),
            "plan": c.plan,
            "seats": c.seats,
            "billing_interval": c.interval,
            "reason": reason or "",
        }
        self.batches[d]["sub"].append(row)
        if d < self.end and self.rng.random() < DUP_SUB_EVENT_PROB:
            self.batches[d + timedelta(days=1)]["sub"].append(dict(row))
            self.issues["duplicated_subscription_events"] += 1

    def issue_invoice(self, c: Customer, d: date):
        self.n_invoices += 1
        months = 1 if c.interval == "monthly" else 12
        amount = c.seats * PRICE[(c.plan, c.interval)] * months
        inv = {
            "invoice_id": f"inv_{self.n_invoices:08d}",
            "customer_id": c.customer_id,
            "issued_date": d.isoformat(),
            "plan": c.plan,
            "seats": c.seats,
            "billing_interval": c.interval,
            "amount_cents": amount,
            "currency": "USD",
        }
        self.batches[d]["inv"].append(inv)
        c.open_invoice = inv
        c.retry_idx = 0
        self.attempt_payment(c, d, first=True)

    def attempt_payment(self, c: Customer, d: date, first: bool):
        inv = c.open_invoice
        if first:
            ok = self.rng.random() >= PAYMENT_FAIL_PROB[c.plan]
        else:
            ok = self.rng.random() < RETRY_SUCCESS_PROB[c.retry_idx - 1]
        self.n_attempts += 1
        self.batches[d]["pay"].append({
            "attempt_id": f"pa_{self.n_attempts:08d}",
            "invoice_id": inv["invoice_id"],
            "customer_id": c.customer_id,
            "attempted_at": self.ts(d).isoformat(sep=" "),
            "status": "succeeded" if ok else "failed",
            "failure_reason": "" if ok else self.rng.choice(["card_declined", "insufficient_funds", "expired_card"]),
        })
        if ok:
            c.open_invoice, c.next_retry = None, None
            return
        self.issues["failed_payment_attempts"] += 1
        if c.retry_idx < len(RETRY_DELAYS):
            c.next_retry = d + timedelta(days=RETRY_DELAYS[c.retry_idx])
            c.retry_idx += 1
        else:
            c.next_retry = None
            self.cancel(c, d, "payment_failed")
            self.issues["involuntary_churn"] += 1

    def cancel(self, c: Customer, d: date, reason: str):
        c.active, c.cancel_date, c.cancel_reason = False, d, reason
        c.open_invoice, c.next_retry, c.next_invoice = None, None, None
        c.seats_at_cancel = c.seats
        self.emit_sub_event(c, d, "canceled", reason)

    def new_customer(self, d: date) -> Customer:
        r = self.rng
        self.n_customers += 1
        plan = r.choices(PLANS, [PLAN_MIX[p] for p in PLANS])[0]
        lo, hi = SEATS_RANGE[plan]
        c = Customer(
            customer_id=f"cus_{self.n_customers:06d}",
            company_name=f"{r.choice(NAME_A)} {r.choice(NAME_B)} {self.n_customers}",
            country=r.choices(COUNTRIES, COUNTRY_W)[0],
            signup_date=d,
            plan=plan,
            seats=r.randint(lo, hi),
            interval="annual" if r.random() < ANNUAL_SHARE[plan] else "monthly",
            anchor_day=min(d.day, 28),
            started_at=d,
        )
        self.customers[c.customer_id] = c
        self.order.append(c.customer_id)
        self.first_start[c.customer_id] = d
        self.emit_sub_event(c, d, "started")
        self.issue_invoice(c, d)
        c.next_invoice = add_months(d, 1 if c.interval == "monthly" else 12, c.anchor_day)
        return c

    # -- daily loop ------------------------------------------------------------
    def step(self, d: date, t: int):
        r = self.rng
        # 1. signups
        for _ in range(poisson(r, SIGNUPS_PER_DAY_START + SIGNUPS_PER_DAY_GROWTH * t)):
            self.new_customer(d)

        for cid in list(self.order):
            c = self.customers[cid]
            if not c.active:
                # reactivation window: 30..365 days after a VOLUNTARY cancel
                if c.cancel_reason == "voluntary" and 30 <= (d - c.cancel_date).days <= 365:
                    if r.random() < REACTIVATION_PROB_DAILY:
                        c.active, c.interval = True, "monthly"
                        c.seats = c.seats_at_cancel
                        c.anchor_day = min(d.day, 28)
                        c.cancel_reason, c.started_at = None, d
                        self.emit_sub_event(c, d, "reactivated")
                        self.issue_invoice(c, d)
                        c.next_invoice = add_months(d, 1, c.anchor_day)
                        self.issues["reactivations"] += 1
                continue

            # 2. payment retries
            if c.next_retry == d:
                self.attempt_payment(c, d, first=False)
                if not c.active:
                    continue

            # 3. renewals / invoices
            if c.next_invoice == d:
                if c.interval == "annual" and r.random() < ANNUAL_RENEWAL_CHURN[c.plan]:
                    self.cancel(c, d, "voluntary")
                    continue
                self.issue_invoice(c, d)
                if not c.active:
                    continue
                c.next_invoice = add_months(d, 1 if c.interval == "monthly" else 12, c.anchor_day)

            # 4. voluntary churn (monthly customers: daily hazard)
            if c.interval == "monthly" and c.open_invoice is None:
                tenure = (d - c.started_at).days
                h = MONTHLY_CHURN[c.plan] * (EARLY_CHURN_MULT if tenure < 90 else 1.0)
                if r.random() < h / 30:
                    self.cancel(c, d, "voluntary")
                    continue

            # 5. seat / plan changes
            changed = False
            if r.random() < SEAT_CHANGE_PROB_MONTHLY / 30:
                delta = r.choice([-2, -1, 1, 1, 2, 3])
                lo, hi = SEATS_RANGE[c.plan]
                new_seats = max(1, c.seats + delta)
                if new_seats != c.seats:
                    c.seats, changed = new_seats, True
                    self.emit_sub_event(c, d, "seats_changed")
            if r.random() < UPGRADE_PROB_MONTHLY / 30 and c.plan != "business":
                c.plan = PLANS[PLANS.index(c.plan) + 1]
                self.emit_sub_event(c, d, "plan_changed", "upgrade")
            elif r.random() < DOWNGRADE_PROB_MONTHLY / 30 and c.plan != "starter":
                c.plan = PLANS[PLANS.index(c.plan) - 1]
                self.emit_sub_event(c, d, "plan_changed", "downgrade")

            # 6. product usage
            p_active = ACTIVE_DAY_PROB_WEEKDAY if d.weekday() < 5 else ACTIVE_DAY_PROB_WEEKEND
            if r.random() < p_active:
                n_users = max(1, int(c.seats * 0.7))
                for _ in range(poisson(r, EVENTS_PER_ACTIVE_DAY)):
                    self.emit_product_event(c.customer_id, d, r.randint(1, n_users))

    def emit_product_event(self, customer_id: str, d: date, user_n: int):
        r = self.rng
        self.n_prod += 1
        orphan = r.random() < ORPHAN_EVENT_PROB
        if orphan:
            self.issues["orphan_product_events"] += 1
        row = {
            "event_id": f"pe_{self.n_prod:09d}",
            "customer_id": "cus_999999" if orphan else customer_id,
            "user_id": f"{customer_id}_u{user_n}",
            "event_name": r.choices(EVENT_NAMES, EVENT_WEIGHTS)[0],
            "event_ts": self.ts(d).isoformat(sep=" "),
        }
        arrival = d
        if r.random() < LATE_EVENT_PROB:
            arrival = d + timedelta(days=r.randint(1, 5))
            self.issues["late_product_events"] += 1
        if arrival > self.end:       # would arrive after the simulation window
            return
        self.batches[arrival]["prod"].append(row)
        if r.random() < DUP_PRODUCT_EVENT_PROB and arrival < self.end:
            self.batches[arrival + timedelta(days=1)]["prod"].append(dict(row))
            self.issues["duplicated_product_events"] += 1

    # -- output ----------------------------------------------------------------
    def customers_dump(self) -> list[dict]:
        rows = []
        for cid in self.order:
            c = self.customers[cid]
            rows.append({
                "customer_id": c.customer_id, "company_name": c.company_name, "country": c.country,
                "signup_date": c.signup_date.isoformat(), "plan": c.plan, "seats": c.seats,
                "billing_interval": c.interval, "status": "active" if c.active else "canceled",
            })
        return rows

    def run(self, out: Path):
        data_dir = out / "data"
        days = (self.end - self.start).days + 1
        dumps_kept: dict[date, list[dict]] = {}
        for t in range(days):
            d = self.start + timedelta(days=t)
            self.step(d, t)
            dumps_kept[d] = self.customers_dump()
            if month_end(d):
                self.month_mrr[d.strftime("%Y-%m")] = {
                    cid: c.mrr() for cid, c in self.customers.items() if c.mrr() > 0
                }
        # write batches (a day can hold rows added by earlier days, so write after the run)
        schemas = {
            "sub": ("subscription_events", ["event_id", "customer_id", "event_type", "event_ts", "plan",
                                            "seats", "billing_interval", "reason"]),
            "inv": ("invoices", ["invoice_id", "customer_id", "issued_date", "plan", "seats",
                                 "billing_interval", "amount_cents", "currency"]),
            "pay": ("payment_attempts", ["attempt_id", "invoice_id", "customer_id", "attempted_at",
                                         "status", "failure_reason"]),
            "prod": ("product_events", ["event_id", "customer_id", "user_id", "event_name", "event_ts"]),
        }
        for t in range(days):
            d = self.start + timedelta(days=t)
            bdir = data_dir / f"batch_date={d.isoformat()}"
            bdir.mkdir(parents=True, exist_ok=True)
            for key, (name, cols) in schemas.items():
                write_csv(bdir / f"{name}.csv", cols, self.batches[d][key], d)
            write_csv(bdir / "customers.csv",
                      ["customer_id", "company_name", "country", "signup_date", "plan", "seats",
                       "billing_interval", "status"], dumps_kept[d], d)

        seeds = out / "seeds"
        seeds.mkdir(parents=True, exist_ok=True)
        with open(seeds / "plan_prices.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["plan", "billing_interval", "price_per_seat_month_cents"])
            for (p, i), v in PRICE.items():
                w.writerow([p, i, v])
        self.write_truth(out / "truth")

    def write_truth(self, tdir: Path):
        tdir.mkdir(parents=True, exist_ok=True)
        months = sorted(self.month_mrr)
        # customer-month MRR
        with open(tdir / "truth_customer_month_mrr.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["month", "customer_id", "mrr_cents"])
            for m in months:
                for cid, v in sorted(self.month_mrr[m].items()):
                    w.writerow([m, cid, v])
        # bridge from month-end snapshots
        seen_before: set[str] = set()
        prev: dict[str, int] = {}
        cohort_of: dict[str, str] = {}
        bridge = []
        for m in months:
            cur = self.month_mrr[m]
            row = dict(month=m, starting_mrr=sum(prev.values()), new_mrr=0, reactivation_mrr=0,
                       expansion_mrr=0, contraction_mrr=0, churned_mrr=0, ending_mrr=sum(cur.values()),
                       active_customers=len(cur), new_customers=0, churned_customers=0)
            for cid in set(prev) | set(cur):
                a, b = prev.get(cid, 0), cur.get(cid, 0)
                if a == 0 and b > 0:
                    if cid in seen_before:
                        row["reactivation_mrr"] += b
                    else:
                        row["new_mrr"] += b
                        row["new_customers"] += 1
                        cohort_of[cid] = m
                elif a > 0 and b == 0:
                    row["churned_mrr"] += a
                    row["churned_customers"] += 1
                elif b > a:
                    row["expansion_mrr"] += b - a
                elif b < a:
                    row["contraction_mrr"] += a - b
            seen_before |= set(cur)
            # NRR / GRR for the month: starting customers only, using month-over-month
            start_ids = [cid for cid, v in prev.items() if v > 0]
            start_total = sum(prev[c] for c in start_ids)
            kept = sum(cur.get(c, 0) for c in start_ids)
            kept_cap = sum(min(cur.get(c, 0), prev[c]) for c in start_ids)
            row["nrr_monthly"] = round(kept / start_total, 6) if start_total else ""
            row["grr_monthly"] = round(kept_cap / start_total, 6) if start_total else ""
            bridge.append(row)
            prev = cur
        with open(tdir / "truth_mrr_monthly.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(bridge[0].keys()))
            w.writeheader()
            w.writerows(bridge)
        # cohort retention (customers, not revenue)
        idx = {m: i for i, m in enumerate(months)}
        cohorts: dict[str, list[str]] = defaultdict(list)
        for cid, m in cohort_of.items():
            cohorts[m].append(cid)
        with open(tdir / "truth_cohort_retention.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["cohort_month", "months_since", "cohort_size", "active_customers"])
            for cm in sorted(cohorts):
                size = len(cohorts[cm])
                for m in months[idx[cm]:]:
                    active = sum(1 for cid in cohorts[cm] if self.month_mrr[m].get(cid, 0) > 0)
                    w.writerow([cm, idx[m] - idx[cm], size, active])
        with open(tdir / "truth_planted_issues.json", "w") as f:
            json.dump(dict(self.issues) | {"customers_created": self.n_customers}, f, indent=2, sort_keys=True)


def write_csv(path: Path, cols: list[str], rows: list[dict], batch: date):
    with open(path, "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(cols + ["_batch_date"])
        for r in rows:
            w.writerow([r[c] for c in cols] + [batch.isoformat()])


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="generated", help="output directory")
    ap.add_argument("--start", default="2024-10-01")
    ap.add_argument("--end", default="2026-09-30")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    g = Generator(date.fromisoformat(a.start), date.fromisoformat(a.end), a.seed)
    g.run(Path(a.out))
    print(f"customers={g.n_customers} sub_events={g.n_events} invoices={g.n_invoices} "
          f"payment_attempts={g.n_attempts} product_events={g.n_prod}")
    print(json.dumps(dict(g.issues), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()