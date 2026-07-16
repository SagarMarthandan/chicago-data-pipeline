-- ============================================================
-- stg_station_status — staging layer for raw Divvy station data
-- ============================================================
-- 1:1 with raw.station_status. Renames columns for clarity,
-- deduplicates on Kafka coordinates (partition + offset), and
-- casts types to match the analytics layer conventions.
--
-- Source: {{ source('raw', 'station_status') }} → raw.station_status
-- Output: staging.stg_station_status (view)
-- ============================================================

SELECT DISTINCT ON (kafka_partition, kafka_offset)
    station_id::text                                       AS station_id,
    num_bikes_available::int                               AS num_bikes_available,
    num_bikes_disabled::int                                AS num_bikes_disabled,
    num_docks_available::int                               AS num_docks_available,
    num_docks_disabled::int                                AS num_docks_disabled,
    is_installed::boolean                                  AS is_installed,
    is_renting::boolean                                    AS is_renting,
    is_returning::boolean                                  AS is_returning,
    last_reported::timestamp                               AS reported_at,
    legacy_id::text                                        AS legacy_id,
    num_ebikes_available::int                              AS num_ebikes_available,
    eightd_has_available_keys::boolean                     AS eightd_has_available_keys,
    num_scooters_available::int                            AS num_scooters_available,
    num_scooters_unavailable::int                          AS num_scooters_unavailable,
    kafka_partition::int                                   AS kafka_partition,
    kafka_offset::bigint                                   AS kafka_offset,
    kafka_timestamp::timestamp                             AS kafka_timestamp,
    ingest_timestamp::timestamp                            AS ingested_at
FROM {{ source('raw', 'station_status') }}
ORDER BY kafka_partition, kafka_offset, ingest_timestamp DESC  -- keep latest ingest per Kafka message
