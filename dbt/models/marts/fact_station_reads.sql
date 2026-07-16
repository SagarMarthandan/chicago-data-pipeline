-- ============================================================
-- fact_station_reads — Divvy station status fact table
-- ============================================================
-- One row per station poll (one Kafka message = one station read).
-- Analytics-ready: supports queries like "avg bikes available at
-- station X over the last hour" or "stations with most downtime".
--
-- Source: {{ ref('stg_station_status') }}
-- Output: mart.fact_station_reads (table)
--
-- Design notes:
--   - date_key links to dim_date for date-based aggregation
--   - reported_at is the station's self-reported timestamp (when
--     the station last sent data to GBFS); ingested_at is when the
--     pipeline received it via Kafka. Use reported_at for analytics
--     about station state, ingested_at for pipeline latency.
--   - station_id stays text (mixed UUID + numeric IDs from GBFS)
-- ============================================================

SELECT
    station_id,
    reported_at,
    reported_at::date                                       AS date_key,
    ingested_at,
    num_bikes_available,
    num_bikes_disabled,
    num_docks_available,
    num_docks_disabled,
    num_ebikes_available,
    num_scooters_available,
    num_scooters_unavailable,
    is_installed,
    is_renting,
    is_returning,
    eightd_has_available_keys,
    -- Derived: total available vehicles (bikes + ebikes + scooters)
    (num_bikes_available + num_ebikes_available
        + COALESCE(num_scooters_available, 0))              AS total_vehicles_available,
    -- Derived: total available docks
    num_docks_available                                     AS total_docks_available,
    -- Kafka traceability
    kafka_partition,
    kafka_offset,
    kafka_timestamp
FROM {{ ref('stg_station_status') }}
WHERE station_id IS NOT NULL
  AND reported_at IS NOT NULL
