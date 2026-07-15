#!/usr/bin/env python3
"""
Divvy GBFS → Kafka Producer — Phase 2.3

Polls the Divvy GBFS station_status feed every 60 seconds and publishes
each station's status as a JSON message to the Kafka topic `divvy_station_status`.

Data flow:
  Divvy GBFS API → this producer → Kafka topic → Spark Structured Streaming → Postgres

Message format:
  Key:   station_id (string, UTF-8 bytes) — ensures same station → same partition
  Value: full station status JSON (UTF-8 bytes)

Why key by station_id:
  Kafka partitions by key hash. Same station_id always goes to the same partition,
  so messages for a given station are ordered. This matters for time-series analysis
  (each station's readings arrive in chronological order).

Usage:
  python kafka/producers/divvy_producer.py                          # defaults: 60s poll, localhost:29092
  python kafka/producers/divvy_producer.py --interval 30            # poll every 30 seconds
  python kafka/producers/divvy_producer.py --bootstrap kafka:9092   # inside Docker network
  python kafka/producers/divvy_producer.py --once                   # single poll (for testing)

GBFS feed:
  URL: https://gbfs.divvybikes.com/gbfs/en/station_status.json
  Refresh: ~60 seconds (TTL in feed)
  Stations: ~2,016 per poll
  No auth required

Graceful shutdown:
  SIGINT (Ctrl-C) or SIGTERM → flushes pending messages, closes producer, exits 0
"""

import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ============================================================
# Configuration
# ============================================================

# Divvy GBFS station status feed (discovered in Phase 2.1)
STATION_STATUS_URL = "https://gbfs.divvybikes.com/gbfs/en/station_status.json"

# Kafka topic for station status messages
TOPIC = "divvy_station_status"

# Default Kafka bootstrap server — host listener for running on host
# Inside Docker, use --bootstrap kafka:9092
DEFAULT_BOOTSTRAP = "localhost:29092"

# Default poll interval — matches GBFS TTL of 60 seconds
DEFAULT_INTERVAL = 60

# HTTP request timeout — GBFS feed is fast, but allow buffer
REQUEST_TIMEOUT = 30

# Logging setup — structured output for observability
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("divvy_producer")


# ============================================================
# Graceful shutdown handling
# ============================================================

# Global flag — set to True by signal handler, checked in main loop
_shutting_down = False


def _handle_signal(signum, frame):
    """Set shutdown flag on SIGINT/SIGTERM."""
    global _shutting_down
    sig_name = signal.Signals(signum).name
    log.info("Received %s — shutting down after current poll...", sig_name)
    _shutting_down = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ============================================================
# Producer
# ============================================================

def create_producer(bootstrap: str, retries: int = 5) -> KafkaProducer:
    """
    Create and return a KafkaProducer connected to the bootstrap server.

    Retries with backoff because Kafka may still be starting up when the
    producer launches (especially in Docker Compose where startup order
    isn't guaranteed for non-dependent services).
    """
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                # Serialize keys and values as UTF-8 bytes
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # Wait for all in-sync replicas to acknowledge — safest delivery
                # guarantee. For a single-broker setup, this is equivalent to acks=1.
                acks="all",
                # Retry on transient failures (network blips, leader election)
                retries=3,
                # Buffer messages if broker is temporarily unavailable
                retry_backoff_ms=500,
            )
            log.info("Connected to Kafka at %s", bootstrap)
            return producer
        except KafkaError:
            log.warning(
                "Cannot connect to Kafka at %s (attempt %d/%d) — retrying in 3s...",
                bootstrap, attempt, retries,
            )
            time.sleep(3)

    log.error("Failed to connect to Kafka after %d attempts. Exiting.", retries)
    sys.exit(1)


def poll_station_status() -> list[dict] | None:
    """
    Fetch station_status from Divvy GBFS API.

    Returns list of station dicts, or None if the request failed.
    Each station dict has: station_id, num_bikes_available, num_docks_available,
    is_renting, is_returning, last_reported, etc. (see docs/knowledge/data-sources.md)
    """
    try:
        response = requests.get(STATION_STATUS_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        stations = data["data"]["stations"]
        # last_updated is the feed's timestamp (epoch seconds)
        feed_last_updated = data.get("last_updated", 0)
        log.info(
            "Fetched %d stations (feed last_updated: %s)",
            len(stations),
            datetime.fromtimestamp(feed_last_updated, tz=timezone.utc).isoformat()
            if feed_last_updated else "unknown",
        )
        return stations
    except requests.RequestException as e:
        log.error("Failed to fetch GBFS feed: %s", e)
        return None
    except (KeyError, json.JSONDecodeError) as e:
        log.error("Malformed GBFS response: %s", e)
        return None


def publish_stations(producer: KafkaProducer, stations: list[dict]) -> int:
    """
    Publish each station's status to the Kafka topic.

    Key = station_id (string) — ensures same station → same partition.
    Value = full station status dict (serialized to JSON bytes).

    Returns the number of messages sent.
    """
    sent = 0
    for station in stations:
        station_id = str(station.get("station_id", ""))
        if not station_id:
            log.warning("Station missing station_id — skipping: %s", station)
            continue

        producer.send(TOPIC, key=station_id, value=station)
        sent += 1

    # Flush ensures all buffered messages are sent before we continue.
    # Without flush, messages may linger in the producer's internal buffer
    # and not be delivered if the process exits immediately after.
    producer.flush()
    return sent


def run_producer(bootstrap: str, interval: int, once: bool):
    """
    Main producer loop: poll GBFS → publish to Kafka → sleep → repeat.

    Args:
        bootstrap: Kafka bootstrap server address
        interval: seconds between polls
        once: if True, do a single poll and exit (for testing)
    """
    producer = create_producer(bootstrap)
    poll_count = 0

    log.info(
        "Starting Divvy producer — topic=%s, bootstrap=%s, interval=%ds, once=%s",
        TOPIC, bootstrap, interval, once,
    )

    while not _shutting_down:
        poll_count += 1
        poll_start = time.time()

        stations = poll_station_status()
        if stations is not None:
            sent = publish_stations(producer, stations)
            elapsed = time.time() - poll_start
            log.info("Poll #%d: sent %d messages in %.2fs", poll_count, sent, elapsed)
        else:
            log.warning("Poll #%d: skipped (fetch failed)", poll_count)

        if once:
            break

        # Sleep for the remaining interval (subtract elapsed time so we
        # poll at a consistent cadence, not interval + fetch time)
        sleep_time = max(0, interval - (time.time() - poll_start))
        if sleep_time > 0 and not _shutting_down:
            time.sleep(sleep_time)

    # Graceful shutdown — flush any remaining buffered messages
    log.info("Shutting down — flushing pending messages...")
    producer.flush(timeout=10)
    producer.close()
    log.info("Producer stopped after %d polls.", poll_count)


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Divvy GBFS → Kafka producer (Phase 2.3)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python kafka/producers/divvy_producer.py                  # run with defaults
  python kafka/producers/divvy_producer.py --once            # single poll (test)
  python kafka/producers/divvy_producer.py --interval 30     # poll every 30s
  python kafka/producers/divvy_producer.py --bootstrap kafka:9092  # inside Docker
        """,
    )
    parser.add_argument(
        "--bootstrap",
        default=DEFAULT_BOOTSTRAP,
        help=f"Kafka bootstrap server (default: {DEFAULT_BOOTSTRAP})",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL,
        help=f"Poll interval in seconds (default: {DEFAULT_INTERVAL})",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Poll once and exit (for testing)",
    )
    args = parser.parse_args()

    run_producer(
        bootstrap=args.bootstrap,
        interval=args.interval,
        once=args.once,
    )


if __name__ == "__main__":
    main()
