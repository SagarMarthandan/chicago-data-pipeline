# Chicago Crime + Divvy Bike-Share Pipeline

A data engineering learning project that answers: **Does crime near a Divvy bike-share station affect ridership?**

## Stack

| Layer | Tool | Phase |
|---|---|---|
| Warehouse | Postgres (local) → BigQuery (cloud) | 1 → 4 |
| Batch processing | Spark DataFrames | 1 |
| Streaming | Kafka + Spark Structured Streaming | 2 |
| Transformation | DBT | 1+ |
| Orchestration | Airflow | 1+ |
| Observability | Grafana | 3 |
| Ingestion (cloud) | Airbyte | 4 |
| Infra (cloud) | Terraform | 4 |
| Containerization | Docker + Docker Compose | 1+ |

## Data Sources

- **Chicago Crime** — Socrata API, ~8M rows, daily batch drops ([data portal](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q4t2))
- **Divvy Bike Share** — GBFS live API, station status every ~60s ([feed](https://gbfs.divvybikes.com/gbfs/gbfs.json))

## Architecture

```mermaid
graph LR
    subgraph Sources
        CC[Chicago Crime API<br/>Socrata ~8M rows]
        DV[Divvy GBFS API<br/>~60s refresh]
    end

    CC -->|batch CSV/parquet| SP[Spark Batch]
    DV -->|live stream| KP[Kafka Producer]
    KP --> KT[(Kafka Topic)]
    KT --> SS[Spark Structured Streaming]

    SP --> PG[(Postgres<br/>raw schema)]
    SS --> PG

    PG --> DBT[DBT<br/>staging → marts]
    DBT --> PG2[(Postgres<br/>mart schema)]

    PG2 --> GR[Grafana<br/>dashboards]
    PG2 --> BI[BI / Analytics]

    AF[Airflow<br/>orchestration] -.-> SP
    AF -.-> DBT
    AF -.-> KP

    style CC fill:#f9d0c4,stroke:#e8744c
    style DV fill:#c4e8f9,stroke:#4c9ee8
    style PG fill:#d4f4dd,stroke:#4ca85a
    style PG2 fill:#d4f4dd,stroke:#4ca85a
    style AF fill:#fff3cd,stroke:#e8c84c
```

## Pipeline Flow

```mermaid
flowchart TD
    A[Download Crime Data<br/>Socrata API] --> B[Spark Batch Job<br/>clean + transform]
    B --> C[Postgres raw.crime_events]
    C --> D[DBT Staging<br/>stg_crime_events]
    D --> E[DBT Marts<br/>fact_crime_events]
    E --> F[DBT Tests<br/>quality checks]
    F --> G[Grafana / Analytics]

    H[Divvy GBFS API] --> I[Kafka Producer]
    I --> J[Kafka Topic]
    J --> K[Spark Streaming]
    K --> L[Postgres raw.station_status]
    L --> M[DBT Staging + Marts]
    M --> G

    style A fill:#f9d0c4
    style H fill:#c4e8f9
    style G fill:#d4f4dd
```

## Roadmap

```mermaid
graph LR
    P1[Phase 1<br/>Batch Foundation<br/>Postgres + Spark + DBT + Airflow]
    P2[Phase 2<br/>Live Stream<br/>Kafka + Spark Streaming]
    P3[Phase 3<br/>Observability<br/>Grafana + DBT Tests + SLAs]
    P4[Phase 4<br/>Cloud Migration<br/>Terraform + BigQuery + Airbyte]

    P1 -->|done when: docker compose up<br/>DAG runs, marts queryable| P2
    P2 -->|done when: live Divvy data<br/>in Postgres via Kafka| P3
    P3 -->|done when: dashboards + tests<br/>+ SLAs operational| P4

    style P1 fill:#fff3cd,stroke:#e8c84c
    style P2 fill:#f0f0f0,stroke:#999
    style P3 fill:#f0f0f0,stroke:#999
    style P4 fill:#f0f0f0,stroke:#999
```

> **Status:** Phase 1 — planning complete, implementation not started

## Phased Build

1. **Batch foundation** — Postgres + Spark batch + DBT marts + Airflow DAG
2. **Live stream** — Divvy GBFS → Kafka → Spark Structured Streaming → Postgres
3. **Observability** — Grafana dashboards, DBT tests, Airflow SLAs
4. **Cloud migration** — Terraform → BigQuery + GCS, Airbyte ingestion

Each phase is a working system before the next begins.

## Project Structure

```
chicago-data-pipeline/
├── docker-compose.yml
├── ingestion/          # Socrata + GBFS data pull
├── spark/              # batch + streaming jobs
├── kafka/              # Divvy producer
├── airflow/            # DAGs
├── dbt/                # staging → intermediate → marts
├── grafana/            # dashboards (Phase 3)
├── terraform/          # cloud infra (Phase 4)
└── docs/               # conventions + learning protocol
```

## Getting Started

```bash
cp .env.example .env    # fill in credentials
docker compose up -d    # start all services
```

See `chicago-pipeline-plan.md` for the full design and `docs/` for engineering conventions.
