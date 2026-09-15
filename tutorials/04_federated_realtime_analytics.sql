-- ============================================================================
-- Tutorial 04 - Federated Real-Time Analytics (Kafka + MySQL + Iceberg)
-- ============================================================================
-- Run:  make tutorial TUT=04
--
-- Business question:
--   A monitoring team asks: "Which patients, RIGHT NOW, have a heart rate that
--   is off their historical baseline? Give me a list with names, and archive
--   any reading above 100 bpm into the long-term store."
--
-- This pipeline touches every layer of the stack in ONE query:
--   kafka  -> gamma telemetry stream (live readings, P#### ids)
--   mysql  -> patient demographics + the P#### bridge
--   nessie -> historical heart-rate baseline (Iceberg observations)
--   nessie -> clinical_archive.high_vitals_history (append-only archive)
-- The star of the show is the federated join: Trino does the work, not any
-- individual data store.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- EXERCISE 1   Bridge catalogs: live reading + demographics.
-- ----------------------------------------------------------------------------
-- The Kafka topic knows patients by 'P####' id; MySQL patients carry both the
-- P#### enrichment (patient_id) and the UUID (id) that the rest of the
-- clinical data uses.

SELECT
    v."patient_id",
    p.first,
    p.last,
    v.heart_rate,
    v.temperature,
    from_unixtime(v."timestamp" / 1000) AS reading_time
FROM kafka.default."telemetry.vitals" v
JOIN mysql.healthcare.patients p
  ON p.patient_id = v."patient_id"
ORDER BY v."timestamp" DESC
LIMIT 10;

-- ----------------------------------------------------------------------------
-- EXERCISE 2   Build each patient's historical baseline (a CTE pipeline).
-- ----------------------------------------------------------------------------
-- Cross the P####/UUID bridge inside a CTE, then aggregate observation history
-- into one baseline row per patient. Only the 8 enriched patients have a
-- mapping, and only a few have heart-rate observations (see the anti-join in
-- EXERCISE 4).

WITH stream_patients AS (
    SELECT patient_id, id, first
    FROM mysql.healthcare.patients
    WHERE patient_id <> ''
),
baseline AS (
    SELECT
        sp.patient_id,
        sp.first AS patient_name,
        round(avg(o.observation_value), 1) AS avg_heart_rate,
        max(o.observation_value)            AS max_heart_rate,
        count(*)                            AS readings
    FROM stream_patients sp
    JOIN nessie.healthcare.observations o
      ON o.patient_id = sp.id
     AND o.description = 'Heart rate'
    GROUP BY 1, 2
)
SELECT *
FROM baseline
ORDER BY patient_id;

-- ----------------------------------------------------------------------------
-- EXERCISE 3   The alert engine: live reading vs. historical baseline.
-- ----------------------------------------------------------------------------
-- Combine the two CTEs with the telemetry stream and flag anyone whose live
-- heart rate is more than 10 bpm above their baseline. The window/CTE work
-- from Tutorials 1-3 all feed the same with-block pipeline.

WITH stream_patients AS (
    SELECT patient_id, id, first
    FROM mysql.healthcare.patients
    WHERE patient_id <> ''
),
baseline AS (
    SELECT
        sp.patient_id,
        sp.first AS patient_name,
        round(avg(o.observation_value), 1) AS avg_heart_rate
    FROM stream_patients sp
    JOIN nessie.healthcare.observations o
      ON o.patient_id = sp.id
     AND o.description = 'Heart rate'
    GROUP BY 1, 2
)
SELECT
    v."patient_id",
    b.patient_name,
    b.avg_heart_rate,
    v.heart_rate,
    round(v.heart_rate - b.avg_heart_rate, 1) AS deviation,
    CASE
        WHEN v.heart_rate > b.avg_heart_rate + 10 THEN 'ALERT'
        ELSE 'ok'
    END AS status
FROM kafka.default."telemetry.vitals" v
JOIN baseline b ON b.patient_id = v."patient_id"
ORDER BY deviation DESC
LIMIT 10;

-- ----------------------------------------------------------------------------
-- EXERCISE 4   Anti-join: stream patients we have NO baseline for.
-- ----------------------------------------------------------------------------
-- A LEFT JOIN + WHERE ... IS NULL returns rows on the LEFT that found NO match
-- on the right. Here it answers: "which live patients have no observation
-- history at all - the ones we cannot baseline yet?" (This also explains why
-- EXERCISE 3 only flags the 3 patients with history.)

WITH stream_patients AS (
    SELECT patient_id, id, first
    FROM mysql.healthcare.patients
    WHERE patient_id <> ''
),
baseline AS (
    SELECT sp.patient_id, round(avg(o.observation_value), 1) AS avg_heart_rate
    FROM stream_patients sp
    JOIN nessie.healthcare.observations o
      ON o.patient_id = sp.id
     AND o.description = 'Heart rate'
    GROUP BY 1
),
stream AS (
    SELECT DISTINCT "patient_id"
    FROM kafka.default."telemetry.vitals"
)
SELECT s."patient_id", b.avg_heart_rate
FROM stream s
LEFT JOIN baseline b ON b.patient_id = s."patient_id"
WHERE b.avg_heart_rate IS NULL
ORDER BY 1;

-- ----------------------------------------------------------------------------
-- EXERCISE 5   Archive high readings into Iceberg (idempotent).
-- ----------------------------------------------------------------------------
-- 'Freeze' live data into the long-term clinical_archive schema. An anti-dup
-- NOT EXISTS guard makes re-runs safe - run this file twice and nothing is
-- inserted twice. Compare the count before/after.

INSERT INTO nessie.clinical_archive.high_vitals_history (patient_id, first, heart_rate, "timestamp")
SELECT
    v."patient_id",
    p.first,
    v.heart_rate,
    v."timestamp"
FROM kafka.default."telemetry.vitals" v
JOIN mysql.healthcare.patients p ON p.patient_id = v."patient_id"
WHERE v.heart_rate > 100
  AND NOT EXISTS (
      SELECT 1
      FROM nessie.clinical_archive.high_vitals_history h
      WHERE h.patient_id = v."patient_id"
        AND h."timestamp" = v."timestamp"
  );

-- Check the archive grew (should include the just-inserted >100 bpm readings):
SELECT count(*) AS archived_rows
FROM nessie.clinical_archive.high_vitals_history;

-- ----------------------------------------------------------------------------
-- EXERCISE 6   Visualize it: Superset.
-- ----------------------------------------------------------------------------
-- 1. Make sure the dashboards exist (idempotent provisioning + data verify):
--      make superset-setup
-- 2. Open http://localhost:8088 (admin/admin) -> the 'Healthcare Lakehouse'
--    dashboard now draws from these exact tables.
-- 3. Build your own time-series chart in Explore:
--      Data > Explore, pick dataset 'healthcare.observations',
--      Chart type = Line Chart,
--      Time column = observation_time,
--      Metric      = AVG(Observation value)  [filter: description = 'Heart rate'],
--      Dimension(s) = description.
--    The Historian chart you build runs the same time-series SQL you just
--    practiced - Superset generates it from the panel, Trino executes it.
-- ============================================================================
-- What you've learned:
--   * federated JOINs cross catalogs in one query (kafka + mysql + nessie)
--   * CTE pipelines mix a bridge table with baselines and live rows
--   * anti-joins (LEFT JOIN ... WHERE ... IS NULL) find 'no match' sets
--   * INSERT ... SELECT with NOT EXISTS = idempotent stream-to-lake ingestion
--   * Superset visualizes the same federated data and lets you prototype
--     time-series charts without writing SQL
-- ============================================================================