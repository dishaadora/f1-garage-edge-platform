# F1 Garage Edge Platform

An edge-to-cloud telemetry platform that simulates the infrastructure problem an
F1 race team faces every weekend: rebuild identical trackside compute at a new
circuit, over an unreliable link, while still delivering season-wide analytics
back at the factory.

> **Status:** Early build. See [Roadmap](#roadmap) for what's done vs. planned.

## The problem

An F1 season runs ~24 races a year, each at a different circuit — sometimes a
different continent the week after. For every one of those weekends, the team:

- Rebuilds its trackside "garage" compute from scratch
- Streams live car telemetry (speed, throttle, brake, gear, GPS, ~10 samples/sec
  per channel) that race engineers need to query **in seconds**, not after a
  cloud round-trip
- Relies on an uplink back to the factory that is often unreliable — shared
  venue infrastructure, satellite backhaul, congested networks
- Still needs that same data permanently archived and queryable across the
  whole season once it's off the local machine

That's a distributed-systems problem wearing a motorsport costume: you need a
**fast, local, offline-tolerant tier** at the track, and a **durable,
aggregated, queryable-at-scale tier** in the cloud, kept in sync whenever the
link allows.

## What this project builds

A reproducible "garage in a box" stack:

- **Terraform** provisions an identical trackside node for any circuit
- **Helm** deploys the ingest + query services onto it as one package
- A **Go** service ingests the telemetry feed and writes it locally as Parquet
- **DuckDB** answers engineer queries against that local data instantly, with
  zero network dependency
- A sync worker ships data to **GCS** whenever connectivity allows, with retry
  logic for a flaky uplink
- **dbt** (on Spark/BigQuery) builds season-wide analytics marts from the
  synced data
- **Prometheus + Grafana** monitor the garage rig itself — ingest lag, disk,
  sync backlog
- **Loki** holds logs for debugging a failed session after the fact

## A note on the data (read this first)

There is no way to attach to a live F1 session as an individual, so this
project **replays historical telemetry** (via [FastF1](https://github.com/theOehrly/Fast-F1))
at real-time or sped-up pace to simulate a session happening right now. The
`replay/` scripts stand in for the actual trackside data feed. Everything
downstream of that — ingest, storage, sync, observability — behaves exactly as
it would against a real feed; only the source is simulated. This is stated
plainly here rather than left for a reviewer to discover.

## Architecture

```
 ┌─────────────────────────── Trackside (per circuit) ───────────────────────────┐
 │                                                                                │
 │   replay/*.py  ──HTTP──▶  Go ingest service  ──writes──▶  Parquet (local)     │
 │  (simulated feed)                                              │              │
 │                                                                 ▼              │
 │                                                         DuckDB (local queries) │
 │                                                                 │              │
 │                                                          sync worker           │
 └─────────────────────────────────────────────────────────────┼────────────────┘
                                                                  │ (when link is up)
                                                                  ▼
 ┌──────────────────────────────── Cloud (AWS) ──────────────────────────────────┐
 │   S3 (raw sync)  ──▶  dbt / Spark  ──▶  season-wide marts (Redshift)          │
 └────────────────────────────────────────────────────────────────────────────────┘

 Observability (both sides): Prometheus ── Grafana ── Loki
 Provisioning: Terraform (nodes) + Helm (services), same chart at every circuit
```

## Tech stack

| Layer | Tools |
|---|---|
| Languages | Python, Go, SQL |
| Local storage / query | Parquet, DuckDB |
| Cloud data | Spark, dbt, GCS, BigQuery |
| Infra | Terraform, Helm, Kubernetes, GCP |
| Observability | Prometheus, Grafana, Loki |

## Repo structure

```
replay/          Python: session exploration + simulated live replay
ingest/          Go: telemetry ingest service (writes local Parquet)
query/           DuckDB-backed query API/CLI
helm/            Helm chart for the trackside stack
terraform/       Provisions a trackside node for any circuit
dbt/             Cloud-side transformation and marts
observability/   Prometheus / Grafana / Loki configs
docs/            Architecture notes and diagrams
```

## Getting started (current state)

Right now, only the data-exploration and replay pieces exist.

```bash
pip install fastf1 pandas pyarrow requests

# Pull one session and inspect its shape (channels, sample rate, size)
python replay/explore_session.py

# Simulate a live feed from a historical session
python replay/replay_session.py --drivers VER HAM --speed 20 \
    --endpoint http://localhost:8080/ingest
```

The Go ingest service that `replay_session.py` posts to doesn't exist yet —
see the roadmap below.

## Roadmap

- [x] Stage 0 — Data exploration (FastF1 session shape, channels, sample rate)
- [x] Stage 0 — Replay engine (simulated live telemetry feed)
- [ ] Stage 1 — Local ingest pipeline (Go service → Parquet → DuckDB queries)
- [ ] Stage 2 — Containerize + Helm chart
- [ ] Stage 3 — Terraform provisioning for trackside nodes
- [ ] Stage 4 — Cloud sync worker + dbt marts
- [ ] Stage 5 — Observability (Prometheus, Grafana, Loki)
- [ ] Stage 6 — Resilience testing (kill the network, kill a pod, fill the disk)

## License

MIT