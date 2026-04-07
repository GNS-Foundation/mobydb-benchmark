# MobyDB Benchmark

> **PostGIS vs MobyDB** — a head-to-head benchmark for geospatial trajectory data.

MobyDB is a purpose-built spatial database for moving objects, built on top of the [GEP (GeoEpoch Protocol)](https://github.com/GNS-Foundation/gep-core) — an H3-native addressing fabric where **location is the key, not a column**.

This benchmark runs identical workloads against both PostGIS and MobyDB and measures:

- Write throughput (batched inserts)
- Spatial range queries (1km radius around a point)
- Point lookups (by public key + epoch)
- Trajectory queries (pubkey across an epoch range)

---

## Architecture

```
benchmark-api (Rust/Axum)
        │
        ├── PostGIS  (PostgreSQL + PostGIS extension)
        │     └── ST_DWithin, GIST index, lat/lon geometry
        │
        └── MobyDB   (Rust, H3-native storage)
              └── /near/:cell, /record/:cell/:epoch/:pubkey
```

### Key difference

PostGIS stores raw coordinates (`POINT(lng lat)`) and builds spatial indexes on top.

MobyDB stores data **indexed natively by H3 cell + epoch + public key** — the address IS the key. There are no lat/lon columns. Privacy is structural, not optional.

---

## Benchmark Tests

### 1. Write Throughput
Inserts N breadcrumbs in batches of 500.

- **PostGIS**: `INSERT INTO breadcrumbs (public_key, h3_cell, epoch, latitude, longitude, ...)`
- **MobyDB**: `POST /write/batch` with signed `MobyRecord` payloads (Ed25519)

### 2. Spatial Range — 1km radius
Finds all records within 1km of Rome center (41.8902, 12.4922).

- **PostGIS**: `ST_DWithin(geom, ST_MakePoint(12.4922, 41.8902)::geography, 1000)`
- **MobyDB**: `GET /near/:h3_cell?rings=1` — H3 ring at resolution 7 ≈ 1km

### 3. Point Lookup
Fetches records for a specific identity.

- **PostGIS**: `WHERE public_key = $1`
- **MobyDB**: `GET /record/:cell/:epoch/:pubkey` — O(1) key lookup

### 4. Trajectory Query
Fetches an identity's movement across an epoch range.

- **PostGIS**: `WHERE public_key = $1 AND epoch BETWEEN $2 AND $3`
- **MobyDB**: Per-epoch key lookups across `(cell, epoch, pubkey)` address space

---

## Data Model

Each benchmark record is a **breadcrumb** — a signed, spatially-addressed movement event:

```json
{
  "address": {
    "h3_cell": 613176081538220031,
    "epoch": 42,
    "public_key": "0042d1dc..."
  },
  "payload": {
    "payload_type": "gns/breadcrumb",
    "data": {
      "lat": 41.8923,
      "lng": 12.4951,
      "altitude_m": 12.4,
      "speed_mps": 1.2
    }
  },
  "signature": "a3f9...",
  "trust_tier": "Navigator"
}
```

Records are **Ed25519-signed** before write. MobyDB verifies signatures on ingest. PostGIS stores raw rows without cryptographic guarantees.

---

## API Endpoints

```
GET  /health                      — System health check
POST /api/seed                    — Seed both databases with test data
POST /api/benchmark/write         — Write throughput test
POST /api/benchmark/spatial-range — 1km spatial range test
POST /api/benchmark/point-lookup  — Single record lookup test
POST /api/benchmark/trajectory    — Epoch range trajectory test
POST /api/benchmark/full          — Run all 4 tests, return summary
GET  /api/stats                   — Record counts in both DBs
GET  /api/results/latest          — Last benchmark run results
```

### Example: Seed 10,000 records around Rome

```bash
curl -X POST https://your-api/api/seed \
  -H "Content-Type: application/json" \
  -d '{
    "entities": 100,
    "points_per_entity": 100,
    "center_lat": 41.8902,
    "center_lng": 12.4922
  }'
```

### Example: Run full benchmark

```bash
curl -X POST https://your-api/api/benchmark/full \
  -H "Content-Type: application/json" \
  -d '{"count": 10000}'
```

Response:

```json
{
  "run_id": "a1b2c3...",
  "dataset_size": 10000,
  "results": [...],
  "summary": {
    "postgis_total_ms": 842.3,
    "mobydb_total_ms": 91.7,
    "speedup_factor": 9.18
  }
}
```

---

## Running Locally

### Prerequisites

- Rust 1.85+
- PostgreSQL with PostGIS extension
- A running MobyDB instance

### Setup

```bash
# Clone
git clone https://github.com/GNS-Foundation/mobydb-benchmark
cd mobydb-benchmark

# Set environment variables
export DATABASE_URL="postgresql://user:pass@localhost/benchmark"
export MOBYDB_URL="http://localhost:7474"
export PORT=3000

# Initialize PostGIS schema
psql $DATABASE_URL -f schema.sql

# Run
cargo run --release
```

### PostGIS Schema

```sql
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE breadcrumbs (
    id          BIGSERIAL PRIMARY KEY,
    public_key  TEXT NOT NULL,
    h3_cell     TEXT NOT NULL,
    epoch       BIGINT NOT NULL,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    altitude_m  DOUBLE PRECISION,
    speed_mps   DOUBLE PRECISION,
    payload     JSONB,
    geom        GEOMETRY(Point, 4326) GENERATED ALWAYS AS
                    (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) STORED,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_breadcrumbs_pubkey  ON breadcrumbs(public_key);
CREATE INDEX idx_breadcrumbs_epoch   ON breadcrumbs(epoch);
CREATE INDEX idx_breadcrumbs_geom    ON breadcrumbs USING GIST(geom);
```

---

## Railway Deployment

This benchmark is deployed on Railway as 3 services:

| Service | Description |
|---|---|
| `benchmark-api` | This Rust/Axum service |
| `postgresql` | Railway PostgreSQL plugin + PostGIS |
| `mobydb` | MobyDB binary from `GNS-Foundation/mobydb` |

Environment variables for `benchmark-api`:

```
DATABASE_URL=${{Postgres.DATABASE_URL}}
MOBYDB_URL=http://mobydb.railway.internal:7474
PORT=3000
```

---

## Why MobyDB?

PostGIS is exceptional for static geospatial data. But for **moving objects with identity**, it forces you to:

- Store raw coordinates (privacy risk)
- Build secondary indexes on identity
- Compute spatial relationships at query time
- Add cryptographic verification as application logic

MobyDB inverts this: **H3 cell + epoch + public key is the primary key**. Spatial proximity is O(1) via H3 arithmetic. Identity lookup is a direct key fetch. Signatures are verified on write.

The benchmark exists to quantify this difference honestly.

---

## Related

- [MobyDB](https://github.com/GNS-Foundation/mobydb) — the database
- [GEP Core](https://github.com/GNS-Foundation/gep-core) — GeoEpoch Protocol
- [GNS Protocol](https://github.com/GNS-Foundation/trip-protocol) — Identity over trajectory

---

*benchmark.mobydb.com — Where the whale beats the elephant.* 🐋
