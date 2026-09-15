-- ============================================================================
-- Tutorial 02 - Window Functions: Trajectories and Deltas
-- ============================================================================
-- Run:  make tutorial TUT=02
--
-- Business question:
--   A quality analyst asks: "For every patient, what was their FIRST and their
--   MOST RECENT heart-rate reading, and how much did heart rate move between
--   consecutive readings? Flag the patients with the biggest single jump."
--
-- Window functions let one row "see" its neighbors WITHOUT collapsing the
-- table into groups. Syntax recap:
--   func(...) OVER (PARTITION BY <one per group> ORDER BY <row order> [ROWS ...])
-- ============================================================================


-- ----------------------------------------------------------------------------
-- EXERCISE 1   Number each reading inside a patient partition.
-- ----------------------------------------------------------------------------
-- row_number() assigns 1,2,3... to each row within each PARTITION BY patient,
-- ordered by observation_time. The patient's history becomes a sequence.

SELECT
    patient_id,
    observation_time,
    observation_value AS bpm,
    row_number() OVER (PARTITION BY patient_id ORDER BY observation_time) AS reading_no
FROM nessie.healthcare.observations
WHERE description = 'Heart rate'
ORDER BY patient_id, reading_no
LIMIT 12;

-- ----------------------------------------------------------------------------
-- EXERCISE 2   First and last reading per patient, without any self-join.
-- ----------------------------------------------------------------------------
-- Trick: keep the ranking from BOTH directions in one pass, then pick the rows
-- that are #1 in each direction inside a GROUP BY.

WITH ranked AS (
    SELECT
        patient_id,
        observation_value,
        row_number() OVER (PARTITION BY patient_id ORDER BY observation_time)      AS rn_first,
        row_number() OVER (PARTITION BY patient_id ORDER BY observation_time DESC) AS rn_last
    FROM nessie.healthcare.observations
    WHERE description = 'Heart rate'
)
SELECT
    patient_id,
    max(CASE WHEN rn_first = 1 THEN observation_value END) AS first_bpm,
    max(CASE WHEN rn_last  = 1 THEN observation_value END) AS last_bpm,
    max(CASE WHEN rn_last  = 1 THEN observation_value END)
        - max(CASE WHEN rn_first = 1 THEN observation_value END) AS drift_bpm
FROM ranked
GROUP BY patient_id
ORDER BY patient_id
LIMIT 10;

-- ----------------------------------------------------------------------------
-- EXERCISE 3   Deltas between consecutive telemetry readings (LAG).
-- ----------------------------------------------------------------------------
-- LAG(x, 1) returns the value of x from the PREVIOUS row in the partition.
-- heart_rate - lag(heart_rate) = the change from reading N-1 to reading N.
--
-- Watch the ORDER BY: the topic's "timestamp" has TIES (multiple readings in
-- the same second). Add _partition_offset as a tie-breaker so the ordering
-- (and therefore LAG) is deterministic - a very real streaming gotcha.

WITH telemetry AS (
    SELECT
        patient_id,
        heart_rate,
        "timestamp",
        lag(heart_rate) OVER (
            PARTITION BY patient_id
            ORDER BY "timestamp", _partition_offset
        ) AS prev_heart_rate
    FROM kafka.default."telemetry.vitals"
)
SELECT
    patient_id,
    from_unixtime("timestamp" / 1000) AS reading_time,
    prev_heart_rate,
    heart_rate,
    heart_rate - prev_heart_rate AS delta
FROM telemetry
WHERE prev_heart_rate IS NOT NULL
ORDER BY patient_id, "timestamp"
LIMIT 12;

-- ----------------------------------------------------------------------------
-- EXERCISE 4   A moving average across a patient's history.
-- ----------------------------------------------------------------------------
-- The window can be a RANGE of rows: ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
-- averages the current reading plus the two before it - i.e. a rolling 3-point
-- average, which smooths noise out of an irregularly-sampled time series.

WITH heart_rates AS (
    SELECT patient_id, observation_time, observation_value
    FROM nessie.healthcare.observations
    WHERE description = 'Heart rate'
)
SELECT
    patient_id,
    observation_time,
    observation_value AS bpm,
    round(avg(observation_value) OVER (
        PARTITION BY patient_id
        ORDER BY observation_time
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 1) AS rolling_avg_3
FROM heart_rates
ORDER BY patient_id, observation_time
LIMIT 12;

-- ----------------------------------------------------------------------------
-- EXERCISE 5   Challenge - the largest single jump in the stream right now.
-- ----------------------------------------------------------------------------
-- Reuse the EXERCISE 3 CTE and rank the absolute value of the delta.

WITH telemetry AS (
    SELECT
        patient_id,
        heart_rate,
        lag(heart_rate) OVER (
            PARTITION BY patient_id
            ORDER BY "timestamp", _partition_offset
        ) AS prev_heart_rate
    FROM kafka.default."telemetry.vitals"
)
SELECT
    patient_id,
    heart_rate,
    prev_heart_rate,
    abs(heart_rate - prev_heart_rate) AS jump
FROM telemetry
WHERE prev_heart_rate IS NOT NULL
ORDER BY jump DESC
LIMIT 5;

-- Empty result check: if a patient has fewer than 2 readings, LAG returns NULL
-- and the row is filtered out. That is by design - you need neighbors to have
-- a delta.

-- ============================================================================
-- What you've learned:
--   * window functions add per-row context WITHOUT collapsing rows (unlike
--     GROUP BY)
--   * PARTITION BY = the group ("per patient"), ORDER BY = the sequence
--   * ROW_NUMBER() gives positions; LAG()/LEAD() give neighbors
--   * ROWS BETWEEN ... PRECEDING AND CURRENT ROW builds rolling windows
--   * nondeterministic ORDER BY (ties) needs an explicit tie-breaker
-- Next: Tutorial 03 uses these on GROUPS OF TIME (monthly buckets) for running
-- totals and year-over-year comparisons.
-- ============================================================================