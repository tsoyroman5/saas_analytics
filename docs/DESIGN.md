# Project 2: SaaS subscription analytics (synthetic data, BigQuery + dbt)

**Goal:** show the part of analytics engineering project 1 does not: business metrics with agreed definitions, incremental models, history tracking, tests that prove the numbers, and CI.

**The data is synthetic and labeled as such everywhere** (README first line, dataset description, dashboard footer). It comes from a seeded generator (`generate_saas_data.py`), so the same seed gives identical files and the true answers are known. That is the point: every metric can be reconciled against ground truth, which real data never allows.

## 1. What the generator produces

24 months (2024-10-01 to 2026-09-30), seed 42: about 2,000 customers, 4.6k subscription events, 13.5k invoices, 14.4k payment attempts, 590k product events. ~120 MB of CSV in 700 daily batch folders.

| Raw table (`raw_saas.*`) | Grain | Notes |
|---|---|---|
| `subscription_events` | one row per state change | `started`, `plan_changed`, `seats_changed`, `canceled`, `reactivated`. Each row carries the full state after the change (plan, seats, interval). Append-only. |
| `invoices` | one row per invoice | monthly or annual, amount in cents |
| `payment_attempts` | one row per attempt | `succeeded`/`failed`; failures retry after 3 and 7 days, then the customer is canceled (`reason = payment_failed`) |
| `product_events` | one row per usage event | arrives in daily batches, some late |
| `customers` | one row per customer, **current state, replaced each load** | the snapshot source |
| `plan_prices` (dbt seed) | plan x interval | price per seat per month in cents; annual = 20% off |

Every file has `_batch_date` (the day the batch was delivered). Raw columns are strings; staging casts them.

### Planted problems (the tests should catch every one)

| Problem | Count (seed 42) | Where it should be handled |
|---|---|---|
| Late product events (`event_ts` 1 to 5 days before the batch that delivers them) | ~18k rows | incremental lookback window |
| Duplicate product events (same `event_id`, next batch) | ~2.8k rows | dedupe on `event_id` in staging |
| Duplicate subscription events | 14 rows | dedupe on `event_id` in staging |
| Orphan product events (`customer_id` not in customers) | ~590 rows | `relationships` test with `severity: warn` plus a quarantine count in the docs |
| Failed payments, retries, involuntary churn | ~1.0k failures, ~120 churned | payment models, `churn_reason` |

Exact counts are in `truth/truth_planted_issues.json` after each run.

## 2. Metric definitions (write these into the dbt docs verbatim)

All metrics are computed from **month-end snapshots**. Changes that start and end inside one month net out; a customer who signs up and cancels in the same month is never counted. This is a deliberate, documented simplification, and the truth files follow it.

- **Customer MRR (month end)** = `seats x price_per_seat(plan, billing_interval)`; 0 when the last event on or before month end is `canceled`. Annual plans use the discounted per-seat price, so this is annual price / 12. Taxes, discounts and usage fees do not exist in this dataset.
- **Total MRR** = sum of customer MRR.
- **MRR bridge**, per month, comparing each customer's MRR with the previous month end:
  - *new*: previous 0, now > 0, never active before
  - *reactivation*: previous 0, now > 0, active in some earlier month
  - *expansion*: previous > 0, now higher; *contraction*: previous > 0, now lower but > 0
  - *churn*: previous > 0, now 0
  - **identity (tested):** `starting + new + reactivation + expansion - contraction - churn = ending`
- **Logo churn rate (month)** = customers churned in month / customers active at previous month end.
- **Gross revenue retention (month)** = sum over last month's customers of `min(now, before)` / last month's MRR.
- **Net revenue retention (month)** = sum over last month's customers of `now` / last month's MRR. Excludes new customers, includes reactivations only of customers who were active last month (so none).
- **Trailing 12-month NRR** = MRR now of customers active 12 months ago / their MRR 12 months ago. This is the number people quote; state clearly that it needs 12 months of history (available from 2025-10).
- **Cohort** = month of a customer's first month-end with MRR > 0. **Cohort retention** (customers) = customers from the cohort with MRR > 0 at month N / cohort size, where month 0 is the cohort month.
- **Involuntary churn** = churn whose last event has `reason = payment_failed`.

## 3. dbt architecture

```
raw_saas.*  ->  staging (views, 1:1, cast + dedupe)
            ->  intermediate
            ->  marts (tables / incremental)
                     metrics (tables)
```

| Layer | Models | Materialization / technique |
|---|---|---|
| staging | `stg_subscription_events`, `stg_invoices`, `stg_payment_attempts`, `stg_product_events`, `stg_customers` | views. Cast types, rename, dedupe on event id (`qualify row_number() ... = 1`, keep the earliest `_batch_date`). |
| intermediate | `int_subscription_periods` (state per customer with `valid_from`/`valid_to` from the event log), `int_customer_month_mrr` (month-end state per customer, 0 after cancel; the model the whole metrics layer builds on), `int_payment_outcomes` (invoice -> final paid/failed) | views or tables; `int_customer_month_mrr` as a table |
| marts | `dim_customer`, `dim_date`, `dim_plan`, `fct_invoice`, `fct_product_events` (**incremental**), `fct_customer_month_mrr` | `fct_product_events`: `incremental`, `insert_overwrite` on a `_batch_date` / `event_date` partition, with a 6-day lookback because events arrive up to 5 days late |
| metrics | `mrr_monthly` (the bridge), `retention_monthly` (logo churn, GRR, NRR), `nrr_trailing_12m`, `cohort_retention`, `product_engagement_monthly` (active customers, active users) | tables, each with documented definitions |
| snapshots | `snap_customers` (check or timestamp strategy on `raw_saas.customers`) | SCD2 history of plan and seat changes **from the first snapshot run onward** |

Two ways to do plan history, and the project should show both on purpose:
1. event-sourced (`int_subscription_periods`): complete history, rebuilt from the event log on every run;
2. snapshot (`snap_customers`): what you do when the source only gives you the current state. It captures only changes seen after it starts running. Say this limitation in the README rather than hiding it.

To make the snapshot meaningful, load the backfill through one date and then advance a day at a time (section 5), running `dbt snapshot` after each load.

## 4. Tests: what makes this project credible

1. **Generic**: `not_null`, `unique` on every key, `accepted_values` (plan, status, interval, event types), `relationships` (facts to dims; orphan product events at `warn`).
2. **Unit tests** (dbt unit tests, small hand-written input rows, expected output): MRR for a customer who upgrades mid-month; a cancel then reactivate across months; an annual customer (MRR = annual/12); the bridge categories for each case; dedupe of a repeated event id.
3. **Source freshness** on `_batch_date` (warn after 2 days, error after 5).
4. **Reconciliation against ground truth**: load `truth/*.csv` into `raw_saas.truth_*` (plain `bq load`, not seeds; the customer-month file has ~25k rows). Singular tests assert that
   - `fct_customer_month_mrr` equals `truth_customer_month_mrr` row for row (zero differences),
   - `mrr_monthly` equals `truth_mrr_monthly` in every column,
   - `cohort_retention` equals `truth_cohort_retention`,
   - the bridge identity holds.
   Run these in a separate `tests/reconciliation/` folder, tagged `truth`, so the production path does not depend on the generator's files.
5. **Singular business rules**: MRR never negative; no invoice for a customer after a non-reactivated cancel; each customer has at most one open subscription.

The README should include a short table of **bugs these tests caught while building** (there will be some; write them down as they happen, with the fix). That table is worth more in an interview than the model list.

## 5. Running it day by day

```
python generate_saas_data.py --out generated            # ~15 s, deterministic
python load_to_bq.py --data generated/data --project <PROJECT> --backfill-through 2026-08-31
dbt seed && dbt build
python load_to_bq.py --data generated/data --project <PROJECT> --day 2026-09-01
dbt snapshot && dbt build                                # incremental run picks up only new data
```

Loads are idempotent: `--day` deletes that `_batch_date` before appending. Re-running a day is safe.

## 6. CI (GitHub Actions)

- **On pull request:** `dbt build --target ci` into a throwaway dataset (`ci_pr_<number>`), then drop it. Uses a BigQuery service account stored in a GitHub secret, with permissions limited to the CI and dev datasets. Never commit keys.
- **Daily (cron):** advance the simulated day, load, `dbt build`, publish docs.
- To keep CI fast and free: CI loads only a small slice (the first 60 days) generated with `--end 2024-11-30`.
- Open question to settle on day 1 of the CI work: how the dbt version you use locally installs in the runner. Pin the exact version in `requirements.txt` and make CI install from it; do not assume the install command from project 1 works unchanged.

## 7. Dashboard

One page, Looker Studio or Metabase (Looker Studio connects to BigQuery with no hosting): MRR and its bridge by month, logo churn and NRR, a cohort retention heatmap, active customers. Footer: "Synthetic data, seed 42". Add the screenshot to the README; do not rely on a live link staying up.

## 8. Plan (about 8 to 10 working days; protect the starred items)

| Day | Work | Done when |
|---|---|---|
| 1 | Create repo, run generator, backfill load, `sources.yml` with freshness, staging models + tests | `dbt build` green on staging |
| 2 | `int_subscription_periods`, `int_customer_month_mrr`, `dim_customer`, `dim_date`, `fct_invoice` | customer-month MRR reconciles with truth |
| 3 | `fct_product_events` incremental with lookback; run it twice and prove the second run touches only recent partitions | row counts match a full refresh |
| 4 | `mrr_monthly`, `retention_monthly`, `cohort_retention`, `nrr_trailing_12m` | all four equal truth |
| 5 * | Unit tests + reconciliation tests; write the "bugs caught" table | tests fail when you break a model on purpose |
| 6 * | Snapshot, daily-advance run, GitHub Actions CI on PRs | green check on a PR |
| 7 | Dashboard, dbt docs on Pages, README with architecture, definitions, limitations, AI-use table | someone can read the README and understand the project in 3 minutes |
| 8 | Buffer + LinkedIn post + resume bullet | published |

Start applications when day 5 is done, as agreed: by then the project already has incremental models, metrics and tests, and CI is two days from finished.

## 9. What is verified and what is not

Verified here: the generator is deterministic (two runs produce byte-identical output), the truth files satisfy the MRR bridge identity in all 24 months, and replaying the subscription events (after dropping duplicate event ids and ordering by `event_ts`) reproduces the true month-end MRR for every customer and month. The loader was run only with `--dry-run`, which checks the file assembly and row counts.

Not verified: the BigQuery load itself (no cloud credentials in this environment), any dbt SQL, the incremental/snapshot behavior on your dbt version, and the CI install command. Expect to fix small things on first run, and tell me what breaks.

## 10. Honest limitations to put in the README

- Synthetic data: results show the pipeline is correct, not that any business behaves this way.
- Month-end snapshots hide intra-month movements.
- One currency, no taxes, discounts, proration, refunds or trials.
- The snapshot only records history from its first run.
- The generator and the dbt models were written by the same person (with AI help), so agreement between them proves consistency of definitions, not that the definitions are the right ones for any real company.
