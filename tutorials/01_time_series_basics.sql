-- ============================================================================
-- Tutorial 01 - Time-Series Fundamentals with CTEs
-- ============================================================================
-- Run:  make tutorial TUT=01
--       (or: docker exec -i trino trino < tutorials/01_time_series_basics.sql)
--
-- Business question:
--   A clinical team lead asks: "Summarize vitals per encounter. I want to know
--   which patients show the widest heart-rate range in their whole history -
--   those are the unstable ones we might watch more closely."
--
-- We work only with nessie.healthcare.observations for most of this tutorial.
-- That table is a classic time series: one row per reading, per encounter,
-- per patient, with a real timestamp.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- EXERCISE 1   Connect to the data.
-- ----------------------------------------------------------------------------
-- Every concept in this tutorial starts with the same idea: filter the
-- time-series table down to one physical measurement first.
-- 'Heart rate' is one of the 'description' values (see Tutorial 3 for a
-- GROUP BY that lists them all).

SELECT patient_id, encounter_id, observation_time, observation_value AS bpm
FROM nessie.healthcare.observations
WHERE description = 'Heart rate'
ORDER BY observation_time DESC
LIMIT 10;

-- ----------------------------------------------------------------------------
-- EXERCISE 2   Aggregate along the time dimension (GROUP BY patient).
-- ----------------------------------------------------------------------------
-- A single row per patient, computed across their whole timeline.
-- count(*)      how many readings they had
-- min/max       the range their heart rate was ever observed in
-- avg           the middle of their history

SELECT
    patient_id,
    count(*)              AS readings,
    min(observation_value)  AS min_bpm,
    max(observation_value)  AS max_bpm,
    round(avg(observation_value), 1) AS avg_bpm
FROM nessie.healthcare.observations
WHERE description = 'Heart rate'
GROUP BY patient_id
ORDER BY readings DESC
LIMIT 10;

-- ----------------------------------------------------------------------------
-- EXERCISE 3   Introduce the CTE: read a query top-down.
-- ----------------------------------------------------------------------------
-- A Common Table Expression (WITH ... AS (...)) names a stage of the query so
-- the rest of the statement reads like a pipeline: FILTER first, AGGREGATE
-- second. From here on every tutorial builds longer pipelines this way.

WITH heart_rates AS (
    SELECT patient_id, observation_time, observation_value
    FROM nessie.healthcare.observations
    WHERE description = 'Heart rate'
)
SELECT
    patient_id,
    count(*) AS readings,
    round(avg(observation_value), 1) AS avg_bpm
FROM heart_rates
GROUP BY patient_id
ORDER BY readings DESC
LIMIT 10;

-- ----------------------------------------------------------------------------
-- EXERCISE 4   Aggregate per encounter: how complete was each visit?
-- ----------------------------------------------------------------------------
-- Here the CTE keeps ALL vital types, and count(*) FILTER (WHERE ...) counts
-- how many readings of each type exist for one encounter. This answers
-- "did we capture a full vital on every visit?"

WITH vitals AS (
    SELECT encounter_id, description
    FROM nessie.healthcare.observations
    WHERE description IN (
        'Heart rate',
        'Respiratory rate',
        'Systolic Blood Pressure',
        'Diastolic Blood Pressure'
    )
)
SELECT
    encounter_id,
    count(*) AS total_readings,
    count(*) FILTER (WHERE description = 'Heart rate')                AS heart_rates,
    count(*) FILTER (WHERE description = 'Respiratory rate')          AS resp_rates,
    count(*) FILTER (WHERE description = 'Systolic Blood Pressure')   AS sbp_readings
FROM vitals
GROUP BY encounter_id
ORDER BY total_readings DESC
LIMIT 10;

-- Note: most encounters record a single set of vitals, which is why counts
-- are usually 1. Encounters with several readings are the ones worth a closer
-- look (e.g. ER or monitoring visits).

-- ----------------------------------------------------------------------------
-- EXERCISE 5   Challenge - the answer to the business question.
-- ----------------------------------------------------------------------------
-- Wide heart-rate range across a patient's history = clinically interesting.
-- HAVING filters the GROUP BY results; note this can return 0 rows if the
-- dataset has no patients that wide (right now it finds several).

WITH heart_rates AS (
    SELECT patient_id, observation_value
    FROM nessie.healthcare.observations
    WHERE description = 'Heart rate'
)
SELECT
    patient_id,
    min(observation_value) AS lo_bpm,
    max(observation_value) AS hi_bpm,
    max(observation_value) - min(observation_value) AS hr_range
FROM heart_rates
GROUP BY patient_id
HAVING max(observation_value) - min(observation_value) >= 20
ORDER BY hr_range DESC
LIMIT 5;

-- ----------------------------------------------------------------------------
-- EXERCISE 6   The streaming side: epoch-milliseconds.
-- ----------------------------------------------------------------------------
-- The same analyst now hears about the live telemetry topic. Notice its
-- timestamps are bigint milliseconds, NOT timestamps:
--   from_unixtime(col / 1000)  ->  human-readable timestamp
-- This is the #1 gotcha when analysts first query a Kafka topic via Trino.

SELECT
    patient_id,
    heart_rate,
    temperature,
    from_unixtime("timestamp" / 1000) AS reading_time
FROM kafka.default."telemetry.vitals"
ORDER BY "timestamp" DESC
LIMIT 10;

-- ============================================================================
-- What you've learned:
--   * a time-series table is just rows ordered by time
--   * aggregates + GROUP BY compress it to per-patient / per-encounter summaries
--   * WITH (CTE) turns a big query into a readable pipeline
--   * count(*) FILTER (WHERE ...) counts within a group
--   * epoch-ms timestamps need from_unixtime(col / 1000)
-- Next: Tutorial 02 turns 'within patient' into 'between consecutive readings'
-- using window functions.
-- ============================================================================