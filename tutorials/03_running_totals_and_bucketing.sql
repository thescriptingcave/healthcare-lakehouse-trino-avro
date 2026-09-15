-- ============================================================================
-- Tutorial 03 - Time Buckets, Running Totals, and Rolling Averages
-- ============================================================================
-- Run:  make tutorial TUT=03
--
-- Business questions:
--   1. Finance: "Give me a monthly encounter-volume and claim-cost burn
--      forecast, with a running total so we can track the year."
--   2. Quality: "Are respiratory rates spiking in any month compared to the
--      trailing 12-month average?"
--
-- New skills:
--   * parse encounter start/stop ISO-8601 STRINGS with from_iso8601_timestamp()
--   * bucket timestamps into months with date_trunc()
--   * turn GROUP BY output into a running total with SUM() OVER (...)
--   * compare a month to the same month last year with LAG(..., 12)
--   * rolling windows over buckets: ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
-- ============================================================================


-- ----------------------------------------------------------------------------
-- EXERCISE 1   Monthly encounter totals (parse + bucket).
-- ----------------------------------------------------------------------------
-- encounters.start is text like '2020-03-14T17:45:28Z', NOT a timestamp type.
-- from_iso8601_timestamp() parses it; date_trunc('month', ...) snaps it to
-- the first of its month so the GROUP BY collapses a month into one row.
-- A WHERE on the parsed timestamp keeps the output recent (the full dataset
-- spans 1912-2020).

SELECT
    date_trunc('month', from_iso8601_timestamp(start)) AS month,
    count(*)               AS encounters,
    round(sum(total_claim_cost), 2) AS claim_cost
FROM nessie.healthcare.encounters
WHERE from_iso8601_timestamp(start) >= from_iso8601_timestamp('2019-01-01T00:00:00Z')
GROUP BY 1
ORDER BY 1;

-- ----------------------------------------------------------------------------
-- EXERCISE 2   Running totals: cumulative volume and cost.
-- ----------------------------------------------------------------------------
-- Wrap the monthly totals in a CTE, then apply a window SUM that accumulates
-- one month at a time. See the finance 'burn' curve take shape.

WITH monthly AS (
    SELECT
        date_trunc('month', from_iso8601_timestamp(start)) AS month,
        count(*) AS encounters,
        round(sum(total_claim_cost), 2) AS claim_cost
    FROM nessie.healthcare.encounters
    WHERE from_iso8601_timestamp(start) >= from_iso8601_timestamp('2019-01-01T00:00:00Z')
    GROUP BY 1
)
SELECT
    date_format(month, '%Y-%m') AS month,
    encounters,
    claim_cost,
    sum(encounters) OVER (ORDER BY month)          AS run_encounters,
    round(sum(claim_cost)  OVER (ORDER BY month), 2) AS run_claim_cost
FROM monthly
ORDER BY month;

-- ----------------------------------------------------------------------------
-- EXERCISE 3   Year-over-year: is volume growing or shrinking?
-- ----------------------------------------------------------------------------
-- lag(encounters, 12) reaches back one year on the ordered monthly series
-- (12 rows = 12 months). nullif() protects against dividing by zero.

WITH monthly AS (
    SELECT
        date_trunc('month', from_iso8601_timestamp(start)) AS month,
        count(*) AS encounters
    FROM nessie.healthcare.encounters
    GROUP BY 1
),
with_last_year AS (
    SELECT
        month,
        encounters,
        lag(encounters, 12) OVER (ORDER BY month) AS encounters_last_year
    FROM monthly
)
SELECT
    date_format(month, '%Y-%m') AS month,
    encounters,
    encounters_last_year,
    round(100.0 * (encounters - encounters_last_year)
          / nullif(encounters_last_year, 0), 1) AS yoy_pct
FROM with_last_year
WHERE month >= from_iso8601_timestamp('2018-01-01T00:00:00Z')
ORDER BY month
LIMIT 8;

-- ----------------------------------------------------------------------------
-- EXERCISE 4   Rolling 12-month average and SPIKE detection.
-- ----------------------------------------------------------------------------
-- Widen the window: ROWS BETWEEN 11 PRECEDING AND CURRENT ROW averages the
-- current month plus the previous 11 = a trailing 12-month baseline. Months
-- whose respiratory-rate average is above that are flagged.
-- (Respiratory-rate readings are sparse, so buckets without readings simply
--  have no rows - the window skips them.)

WITH respiratory_rates AS (
    SELECT
        date_trunc('month', observation_time) AS month,
        round(avg(observation_value), 2) AS avg_rr
    FROM nessie.healthcare.observations
    WHERE description = 'Respiratory rate'
    GROUP BY 1
)
SELECT
    date_format(month, '%Y-%m') AS month,
    avg_rr,
    round(avg(avg_rr) OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW), 2) AS trailing_12mo,
    CASE
        WHEN avg_rr > avg(avg_rr) OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)
        THEN 'SPIKE'
        ELSE 'normal'
    END AS flag
FROM respiratory_rates
ORDER BY month DESC
LIMIT 8;

-- ----------------------------------------------------------------------------
-- EXERCISE 5   Challenge - cost share by encounter class over time.
-- ----------------------------------------------------------------------------
-- Combine everything: bucket by YEAR and encounter class, then add a running
-- total per class. Answer: "how does each visit type's yearly spend accumulate
-- vs. the previous year?"

WITH yearly AS (
    SELECT
        encounterclass,
        date_trunc('year', from_iso8601_timestamp(start)) AS year,
        round(sum(total_claim_cost), 2) AS cost
    FROM nessie.healthcare.encounters
    WHERE from_iso8601_timestamp(start) >= from_iso8601_timestamp('2016-01-01T00:00:00Z')
    GROUP BY 1, 2
)
SELECT
    encounterclass,
    date_format(year, '%Y') AS year,
    cost,
    round(sum(cost) OVER (PARTITION BY encounterclass ORDER BY year), 2) AS cumulative_cost
FROM yearly
ORDER BY encounterclass, year
LIMIT 12;

-- ============================================================================
-- What you've learned:
--   * text timestamps parse with from_iso8601_timestamp(); buckets snap with
--     date_trunc()
--   * SUM() OVER (ORDER BY bucket) = running/cumulative totals
--   * LAG(..., N) compares against N buckets earlier (YoY = 12)
--   * ROWS BETWEEN N PRECEDING AND CURRENT ROW = rolling average window
--   * CASE WHEN over a window value = flagging engines
-- Next: Tutorial 04 joins Kafka + MySQL + Iceberg into one real-time alert
-- pipeline and visualizes the result in Superset.
-- ============================================================================