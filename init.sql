-- PostGIS benchmark schema
-- Run this after enabling PostGIS extension on Railway PostgreSQL

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- ============================================================
-- Table: breadcrumbs (mobility/IoT trajectory points)
-- Mirrors MobyDB's (H3 cell, epoch, pubkey) composite key
-- ============================================================
CREATE TABLE IF NOT EXISTS breadcrumbs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    public_key      TEXT NOT NULL,           -- Ed25519 hex
    h3_cell         TEXT NOT NULL,           -- H3 index string (res 7)
    epoch           BIGINT NOT NULL,         -- GEP epoch
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,
    geom            GEOMETRY(Point, 4326),   -- PostGIS point
    altitude_m      DOUBLE PRECISION DEFAULT 0,
    speed_mps       DOUBLE PRECISION DEFAULT 0,
    payload         JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Spatial index (the whole point of the benchmark)
CREATE INDEX IF NOT EXISTS idx_breadcrumbs_geom 
    ON breadcrumbs USING GIST (geom);

-- Composite index for temporal-spatial queries
CREATE INDEX IF NOT EXISTS idx_breadcrumbs_epoch 
    ON breadcrumbs (epoch);

CREATE INDEX IF NOT EXISTS idx_breadcrumbs_pubkey 
    ON breadcrumbs (public_key);

CREATE INDEX IF NOT EXISTS idx_breadcrumbs_h3 
    ON breadcrumbs (h3_cell);

CREATE INDEX IF NOT EXISTS idx_breadcrumbs_pubkey_epoch 
    ON breadcrumbs (public_key, epoch);

-- ============================================================
-- Table: regions (polygons for containment queries)
-- ============================================================
CREATE TABLE IF NOT EXISTS regions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    geom            GEOMETRY(Polygon, 4326),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_regions_geom 
    ON regions USING GIST (geom);

-- ============================================================
-- Benchmark helper: auto-populate geom from lat/lng on insert
-- ============================================================
CREATE OR REPLACE FUNCTION set_breadcrumb_geom()
RETURNS TRIGGER AS $$
BEGIN
    NEW.geom := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_breadcrumb_geom ON breadcrumbs;
CREATE TRIGGER trg_breadcrumb_geom
    BEFORE INSERT OR UPDATE ON breadcrumbs
    FOR EACH ROW
    EXECUTE FUNCTION set_breadcrumb_geom();

-- ============================================================
-- Seed some benchmark regions (major cities)
-- ============================================================
INSERT INTO regions (name, geom) VALUES
    ('Rome Centro', ST_SetSRID(ST_MakePolygon(ST_GeomFromText(
        'LINESTRING(12.45 41.87, 12.52 41.87, 12.52 41.92, 12.45 41.92, 12.45 41.87)'
    )), 4326)),
    ('Milan Centro', ST_SetSRID(ST_MakePolygon(ST_GeomFromText(
        'LINESTRING(9.15 45.44, 9.22 45.44, 9.22 45.50, 9.15 45.50, 9.15 45.44)'
    )), 4326)),
    ('NYC Manhattan', ST_SetSRID(ST_MakePolygon(ST_GeomFromText(
        'LINESTRING(-74.02 40.70, -73.93 40.70, -73.93 40.80, -74.02 40.80, -74.02 40.70)'
    )), 4326)),
    ('London City', ST_SetSRID(ST_MakePolygon(ST_GeomFromText(
        'LINESTRING(-0.15 51.49, -0.05 51.49, -0.05 51.54, -0.15 51.54, -0.15 51.49)'
    )), 4326))
ON CONFLICT DO NOTHING;
