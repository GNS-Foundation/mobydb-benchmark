use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use ed25519_dalek::{Signer, SigningKey};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Instant;
use tower_http::cors::CorsLayer;
use tracing::{info, warn};

// ── App state ──────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    pg: PgPool,
    mobydb_url: String,
}

// ── Models ─────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Clone)]
struct BenchmarkResult {
    test_name: String,
    engine: String, // "PostGIS" or "MobyDB"
    rows_affected: i64,
    duration_ms: f64,
    ops_per_sec: f64,
    timestamp: String,
}

#[derive(Debug, Serialize)]
struct BenchmarkSuite {
    run_id: String,
    started_at: String,
    dataset_size: i64,
    results: Vec<BenchmarkResult>,
    summary: BenchmarkSummary,
}

#[derive(Debug, Serialize)]
struct BenchmarkSummary {
    postgis_total_ms: f64,
    mobydb_total_ms: f64,
    speedup_factor: f64,
}

#[derive(Debug, Deserialize, Clone)]
struct RunParams {
    #[serde(default = "default_count")]
    count: i64,
}

fn default_count() -> i64 {
    10_000
}

#[derive(Debug, Deserialize, Serialize)]
struct SeedParams {
    #[serde(default = "default_entities")]
    entities: u32,
    #[serde(default = "default_points_per")]
    points_per_entity: u32,
    #[serde(default = "default_lat")]
    center_lat: f64,
    #[serde(default = "default_lng")]
    center_lng: f64,
}

fn default_entities() -> u32 {
    100
}
fn default_points_per() -> u32 {
    100
}
fn default_lat() -> f64 {
    41.8902
} // Rome
fn default_lng() -> f64 {
    12.4922
}

#[derive(Debug, Serialize)]
struct SeedResult {
    postgis_rows: u64,
    mobydb_rows: u64,
    postgis_seed_ms: f64,
    mobydb_seed_ms: f64,
    total_points: u64,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    postgis: &'static str,
    mobydb: &'static str,
}

// ── Breadcrumb with signing context ────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Breadcrumb {
    public_key: String,
    h3_cell: String,
    h3_cell_u64: u64,
    epoch: i64,
    latitude: f64,
    longitude: f64,
    altitude_m: f64,
    speed_mps: f64,
    payload: serde_json::Value,
    /// Signing key bytes (hex) — used to construct signed MobyRecords
    #[serde(skip_serializing)]
    signing_key_hex: String,
}

impl Breadcrumb {
    /// Convert to a signed MobyRecord JSON value for MobyDB's /write/batch API
    fn to_moby_record(&self) -> serde_json::Value {
        let sk_bytes = hex::decode(&self.signing_key_hex).unwrap();
        let sk_array: [u8; 32] = sk_bytes.try_into().unwrap();
        let signing_key = SigningKey::from_bytes(&sk_array);
        let pk_bytes: [u8; 32] = signing_key.verifying_key().to_bytes();

        let written_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let data = serde_json::json!({
            "lat": self.latitude,
            "lng": self.longitude,
            "altitude_m": self.altitude_m,
            "speed_mps": self.speed_mps,
            "h3_cell_hex": self.h3_cell,
        });

        // Build canonical bytes (must match MobyDB's MobyRecord::canonical_bytes)
        let mut canon = serde_json::Map::new();
        canon.insert("data".into(), data.clone());
        canon.insert("epoch".into(), serde_json::json!(self.epoch as u64));
        canon.insert("h3_cell".into(), serde_json::json!(self.h3_cell_u64));
        canon.insert("payload_type".into(), serde_json::json!("gns/breadcrumb"));
        canon.insert(
            "public_key".into(),
            serde_json::json!(hex::encode(pk_bytes)),
        );
        canon.insert("written_at_ms".into(), serde_json::json!(written_at_ms));

        let canon_bytes =
            serde_json::to_vec(&serde_json::Value::Object(canon)).unwrap_or_default();
        let signature = signing_key.sign(&canon_bytes);

        serde_json::json!({
            "address": {
                "h3_cell": self.h3_cell_u64,
                "epoch": self.epoch as u64,
                "public_key": hex::encode(pk_bytes)
            },
            "payload": {
                "collection_type": "breadcrumb",
                "payload_type": "gns/breadcrumb",
                "data": data
            },
            "signature": hex::encode(signature.to_bytes()),
            "trust_tier": "Navigator",
            "written_at_ms": written_at_ms
        })
    }
}

// ── Main ───────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("benchmark_api=debug,info")
        .init();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let mobydb_url =
        std::env::var("MOBYDB_URL").unwrap_or_else(|_| "http://localhost:7474".to_string());
    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());

    info!("Connecting to PostGIS (lazy)...");
    let pg = PgPoolOptions::new()
        .max_connections(10)
        .connect_lazy(&database_url)
        .expect("Failed to create PostgreSQL pool");

    info!("PostGIS pool created (lazy)");
    info!("MobyDB target: {}", mobydb_url);

    let state = AppState { pg, mobydb_url };

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/api/seed", post(seed_data))
        .route("/api/benchmark/write", post(bench_write))
        .route("/api/benchmark/spatial-range", post(bench_spatial_range))
        .route("/api/benchmark/point-lookup", post(bench_point_lookup))
        .route("/api/benchmark/trajectory", post(bench_trajectory))
        .route("/api/benchmark/full", post(bench_full_suite))
        .route("/api/results/latest", get(get_latest_results))
        .route("/api/stats", get(get_stats))
        .layer(CorsLayer::permissive())
        .with_state(Arc::new(state));

    let addr = format!("0.0.0.0:{}", port);
    info!("Benchmark API listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// ── Health ─────────────────────────────────────────────────

async fn health_check(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    let pg_ok = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        sqlx::query("SELECT 1").fetch_one(&state.pg),
    )
    .await
    .map(|r| r.is_ok())
    .unwrap_or(false);

    let moby_ok = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        reqwest::get(format!("{}/health", state.mobydb_url)),
    )
    .await
    .map(|r| r.map(|r| r.status().is_success()).unwrap_or(false))
    .unwrap_or(false);

    Json(HealthResponse {
        status: "ok",
        postgis: if pg_ok { "connected" } else { "down" },
        mobydb: if moby_ok { "connected" } else { "down" },
    })
}

// ── Stats ──────────────────────────────────────────────────

async fn get_stats(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let pg_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM breadcrumbs")
        .fetch_one(&state.pg)
        .await
        .unwrap_or((0,));

    let moby_stats = match reqwest::get(format!("{}/stats", state.mobydb_url)).await {
        Ok(r) if r.status().is_success() => r
            .json::<serde_json::Value>()
            .await
            .unwrap_or_else(|_| serde_json::json!({"total_breadcrumbs": 0})),
        _ => serde_json::json!({"total_breadcrumbs": 0}),
    };

    Json(serde_json::json!({
        "postgis": { "breadcrumbs": pg_count.0 },
        "mobydb": moby_stats,
    }))
}

// ── Seed Data ──────────────────────────────────────────────

async fn seed_data(
    State(state): State<Arc<AppState>>,
    Json(params): Json<SeedParams>,
) -> Json<SeedResult> {
    let total = params.entities as u64 * params.points_per_entity as u64;
    info!(
        "Seeding {} breadcrumbs ({} entities x {} points)...",
        total, params.entities, params.points_per_entity
    );

    let breadcrumbs = generate_breadcrumbs(&params);

    // ── Seed PostGIS ───────────────────────────────────────
    let pg_start = Instant::now();
    let mut pg_rows = 0u64;

    for chunk in breadcrumbs.chunks(500) {
        let mut query = String::from(
            "INSERT INTO breadcrumbs (public_key, h3_cell, epoch, latitude, longitude, altitude_m, speed_mps, payload) VALUES ",
        );
        let mut values = Vec::new();
        for (i, b) in chunk.iter().enumerate() {
            if i > 0 {
                query.push(',');
            }
            let offset = i * 8;
            query.push_str(&format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                offset + 1,
                offset + 2,
                offset + 3,
                offset + 4,
                offset + 5,
                offset + 6,
                offset + 7,
                offset + 8
            ));
            values.push(b.clone());
        }

        let mut q = sqlx::query(&query);
        for b in &values {
            q = q
                .bind(&b.public_key)
                .bind(&b.h3_cell)
                .bind(b.epoch)
                .bind(b.latitude)
                .bind(b.longitude)
                .bind(b.altitude_m)
                .bind(b.speed_mps)
                .bind(&b.payload);
        }

        match q.execute(&state.pg).await {
            Ok(r) => pg_rows += r.rows_affected(),
            Err(e) => warn!("PostGIS insert error: {}", e),
        }
    }
    let pg_ms = pg_start.elapsed().as_secs_f64() * 1000.0;
    info!("PostGIS: {} rows in {:.1}ms", pg_rows, pg_ms);

    // ── Seed MobyDB ────────────────────────────────────────
    let moby_start = Instant::now();
    let mut moby_rows = 0u64;
    let client = reqwest::Client::new();

    for chunk in breadcrumbs.chunks(500) {
        let moby_records: Vec<serde_json::Value> =
            chunk.iter().map(|b| b.to_moby_record()).collect();

        match client
            .post(format!("{}/write/batch", state.mobydb_url))
            .json(&moby_records)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {
                moby_rows += chunk.len() as u64;
            }
            Ok(r) => {
                let status = r.status();
                let body = r.text().await.unwrap_or_default();
                warn!("MobyDB batch write returned {} — {}", status, body);
            }
            Err(e) => warn!("MobyDB write error: {}", e),
        }
    }
    let moby_ms = moby_start.elapsed().as_secs_f64() * 1000.0;
    info!("MobyDB: {} rows in {:.1}ms", moby_rows, moby_ms);

    Json(SeedResult {
        postgis_rows: pg_rows,
        mobydb_rows: moby_rows,
        postgis_seed_ms: pg_ms,
        mobydb_seed_ms: moby_ms,
        total_points: total,
    })
}

// ── Benchmark: Write throughput ────────────────────────────

async fn bench_write(
    State(state): State<Arc<AppState>>,
    Json(params): Json<RunParams>,
) -> Json<Vec<BenchmarkResult>> {
    let seed_params = SeedParams {
        entities: 10,
        points_per_entity: (params.count / 10) as u32,
        center_lat: 41.8902,
        center_lng: 12.4922,
    };
    let breadcrumbs = generate_breadcrumbs(&seed_params);
    let mut results = Vec::new();

    // PostGIS write
    let start = Instant::now();
    let mut rows = 0i64;
    for chunk in breadcrumbs.chunks(500) {
        let mut query = String::from(
            "INSERT INTO breadcrumbs (public_key, h3_cell, epoch, latitude, longitude, altitude_m, speed_mps) VALUES ",
        );
        for (i, _) in chunk.iter().enumerate() {
            if i > 0 {
                query.push(',');
            }
            let o = i * 7;
            query.push_str(&format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${})",
                o + 1,
                o + 2,
                o + 3,
                o + 4,
                o + 5,
                o + 6,
                o + 7
            ));
        }
        let mut q = sqlx::query(&query);
        for b in chunk {
            q = q
                .bind(&b.public_key)
                .bind(&b.h3_cell)
                .bind(b.epoch)
                .bind(b.latitude)
                .bind(b.longitude)
                .bind(b.altitude_m)
                .bind(b.speed_mps);
        }
        if let Ok(r) = q.execute(&state.pg).await {
            rows += r.rows_affected() as i64;
        }
    }
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        test_name: "write_throughput".into(),
        engine: "PostGIS".into(),
        rows_affected: rows,
        duration_ms: ms,
        ops_per_sec: rows as f64 / (ms / 1000.0),
        timestamp: Utc::now().to_rfc3339(),
    });

    // MobyDB write — proper MobyRecord format
    let start = Instant::now();
    let mut rows = 0i64;
    let client = reqwest::Client::new();
    for chunk in breadcrumbs.chunks(500) {
        let moby_records: Vec<serde_json::Value> =
            chunk.iter().map(|b| b.to_moby_record()).collect();

        if let Ok(r) = client
            .post(format!("{}/write/batch", state.mobydb_url))
            .json(&moby_records)
            .send()
            .await
        {
            if r.status().is_success() {
                rows += chunk.len() as i64;
            }
        }
    }
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        test_name: "write_throughput".into(),
        engine: "MobyDB".into(),
        rows_affected: rows,
        duration_ms: ms,
        ops_per_sec: rows as f64 / (ms / 1000.0),
        timestamp: Utc::now().to_rfc3339(),
    });

    Json(results)
}

// ── Benchmark: Spatial range query ─────────────────────────

async fn bench_spatial_range(
    State(state): State<Arc<AppState>>,
    Json(_params): Json<RunParams>,
) -> Json<Vec<BenchmarkResult>> {
    let mut results = Vec::new();

    // PostGIS: ST_DWithin (find points within 1km of Rome center)
    let start = Instant::now();
    let pg_rows: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM breadcrumbs WHERE ST_DWithin(geom, ST_SetSRID(ST_MakePoint(12.4922, 41.8902), 4326)::geography, 1000)"
    )
    .fetch_one(&state.pg)
    .await
    .unwrap_or((0,));
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        test_name: "spatial_range_1km".into(),
        engine: "PostGIS".into(),
        rows_affected: pg_rows.0,
        duration_ms: ms,
        ops_per_sec: 1000.0 / ms,
        timestamp: Utc::now().to_rfc3339(),
    });

    // MobyDB: /near/:cell — query by H3 cell (Rome center at res 7, with 1 ring for ~1km)
    let center_cell_hex = {
        let ll = h3o::LatLng::new(41.8902, 12.4922).unwrap();
        let cell = ll.to_cell(h3o::Resolution::Seven);
        cell.to_string() // hex format like "871e8052affffff"
    };

    let start = Instant::now();
    let moby_count = match reqwest::get(format!(
        "{}/near/{}?rings=1&epoch_start=1&epoch_end=100000&limit=10000",
        state.mobydb_url, center_cell_hex
    ))
    .await
    {
        Ok(r) if r.status().is_success() => {
            let body: serde_json::Value = r.json().await.unwrap_or_default();
            // MobyDB wraps response in { success, data: { count, records }, error }
            if let Some(data) = body.get("data") {
                if let Some(count) = data.get("count").and_then(|c| c.as_i64()) {
                    count
                } else if let Some(records) = data.get("records").and_then(|r| r.as_array()) {
                    records.len() as i64
                } else {
                    0
                }
            } else {
                0
            }
        }
        Ok(r) => {
            let status = r.status();
            let body = r.text().await.unwrap_or_default();
            warn!("MobyDB near query returned {} — {}", status, body);
            0
        }
        Err(e) => {
            warn!("MobyDB near query error: {}", e);
            0
        }
    };
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        test_name: "spatial_range_1km".into(),
        engine: "MobyDB".into(),
        rows_affected: moby_count,
        duration_ms: ms,
        ops_per_sec: 1000.0 / ms,
        timestamp: Utc::now().to_rfc3339(),
    });

    Json(results)
}

// ── Benchmark: Point lookup by key ─────────────────────────

async fn bench_point_lookup(
    State(state): State<Arc<AppState>>,
    Json(_params): Json<RunParams>,
) -> Json<Vec<BenchmarkResult>> {
    let mut results = Vec::new();

    // Get a sample public key + cell + epoch from PostGIS
    let sample: Option<(String, String, i64)> = sqlx::query_as(
        "SELECT public_key, h3_cell, epoch FROM breadcrumbs ORDER BY id DESC LIMIT 1",
    )
    .fetch_optional(&state.pg)
    .await
    .unwrap_or(None);

    let (pubkey, h3_cell_str, epoch) = match sample {
        Some(s) => s,
        None => return Json(results),
    };

    // PostGIS: index lookup by pubkey
    let start = Instant::now();
    let pg_rows: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM breadcrumbs WHERE public_key = $1")
            .bind(&pubkey)
            .fetch_one(&state.pg)
            .await
            .unwrap_or((0,));
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        test_name: "point_lookup_by_pubkey".into(),
        engine: "PostGIS".into(),
        rows_affected: pg_rows.0,
        duration_ms: ms,
        ops_per_sec: 1000.0 / ms,
        timestamp: Utc::now().to_rfc3339(),
    });

    // MobyDB: /record/:cell/:epoch/:pubkey — exact record lookup
    // h3_cell from PostGIS is already hex format (e.g. "871e8052affffff")
    let start = Instant::now();
    // Convert hex h3_cell to u64 — MobyDB address uses u64
    let h3_cell = match h3_cell_str.parse::<h3o::CellIndex>() {
        Ok(c) => c,
        Err(e) => {
            warn!("point_lookup: failed to parse h3_cell '{}': {}", h3_cell_str, e);
            return Json(results);
        }
    };
    let h3_u64 = u64::from(h3_cell);
    let moby_found = match reqwest::get(format!(
        "{}/record/{}/{}/{}",
        state.mobydb_url, h3_u64, epoch, pubkey
    ))
    .await
    {
        Ok(r) if r.status().is_success() => {
            let body: serde_json::Value = r.json().await.unwrap_or_default();
            if body.get("success").and_then(|s| s.as_bool()).unwrap_or(false) {
                1i64
            } else {
                0
            }
        }
        _ => 0,
    };
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        test_name: "point_lookup_by_pubkey".into(),
        engine: "MobyDB".into(),
        rows_affected: moby_found,
        duration_ms: ms,
        ops_per_sec: 1000.0 / ms,
        timestamp: Utc::now().to_rfc3339(),
    });

    Json(results)
}

// ── Benchmark: Trajectory query (pubkey + epoch range) ─────

async fn bench_trajectory(
    State(state): State<Arc<AppState>>,
    Json(_params): Json<RunParams>,
) -> Json<Vec<BenchmarkResult>> {
    let mut results = Vec::new();

    // Get sample pubkey with epoch range
    let sample: Option<(String, i64, i64)> = sqlx::query_as(
        "SELECT public_key, MIN(epoch), MAX(epoch) FROM breadcrumbs WHERE id > (SELECT MAX(id) - 10000 FROM breadcrumbs) GROUP BY public_key LIMIT 1",
    )
    .fetch_optional(&state.pg)
    .await
    .unwrap_or(None);

    let (pubkey, min_epoch, max_epoch) = match sample {
        Some(s) => s,
        None => return Json(results),
    };

    let mid = (min_epoch + max_epoch) / 2;

    // PostGIS: pubkey + epoch range
    let start = Instant::now();
    let pg_rows: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM breadcrumbs WHERE public_key = $1 AND epoch BETWEEN $2 AND $3",
    )
    .bind(&pubkey)
    .bind(min_epoch)
    .bind(mid)
    .fetch_one(&state.pg)
    .await
    .unwrap_or((0,));
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        test_name: "trajectory_query".into(),
        engine: "PostGIS".into(),
        rows_affected: pg_rows.0,
        duration_ms: ms,
        ops_per_sec: 1000.0 / ms,
        timestamp: Utc::now().to_rfc3339(),
    });

    // MobyDB: fetch individual records across epochs
    // MobyDB's native key is (cell, epoch, pubkey) — we look up multiple epochs
    // For a fair comparison, we query each epoch point individually
    let start = Instant::now();

    // Get the h3_cells for this pubkey from PostGIS to know which cells to query in MobyDB
    let cell_epochs: Vec<(String, i64)> = sqlx::query_as(
        "SELECT h3_cell, epoch FROM breadcrumbs WHERE public_key = $1 AND epoch BETWEEN $2 AND $3",
    )
    .bind(&pubkey)
    .bind(min_epoch)
    .bind(mid)
    .fetch_all(&state.pg)
    .await
    .unwrap_or_default();

    let client = reqwest::Client::new();
    let futs: Vec<_> = cell_epochs.iter().filter_map(|(cell_str, ep)| {
        let h3_u64 = match cell_str.parse::<h3o::CellIndex>() {
            Ok(c) => u64::from(c),
            Err(_) => return None,
        };
        let url = format!("{}/record/{}/{}/{}", state.mobydb_url, h3_u64, ep, pubkey);
        let c = client.clone();
        Some(async move { c.get(&url).send().await })
    }).collect();

    let responses = futures::future::join_all(futs).await;
    let moby_count = responses.iter()
        .filter(|r| r.as_ref().map(|r| r.status().is_success()).unwrap_or(false))
        .count() as i64;
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        test_name: "trajectory_query".into(),
        engine: "MobyDB".into(),
        rows_affected: moby_count,
        duration_ms: ms,
        ops_per_sec: 1000.0 / ms,
        timestamp: Utc::now().to_rfc3339(),
    });

    Json(results)
}

// ── Full benchmark suite ───────────────────────────────────

async fn bench_full_suite(
    State(state): State<Arc<AppState>>,
    Json(params): Json<RunParams>,
) -> Json<BenchmarkSuite> {
    let run_id = uuid::Uuid::new_v4().to_string();
    let started_at = Utc::now().to_rfc3339();
    let mut all_results = Vec::new();

    info!("Starting full benchmark suite: {}", run_id);

    // 1. Write throughput
    let write_results = bench_write(State(state.clone()), Json(params.clone()))
        .await
        .0;
    all_results.extend(write_results);

    // 2. Spatial range
    let spatial = bench_spatial_range(State(state.clone()), Json(params.clone()))
        .await
        .0;
    all_results.extend(spatial);

    // 3. Point lookup
    let lookup = bench_point_lookup(State(state.clone()), Json(params.clone()))
        .await
        .0;
    all_results.extend(lookup);

    // 4. Trajectory
    let traj = bench_trajectory(State(state.clone()), Json(params.clone()))
        .await
        .0;
    all_results.extend(traj);

    // Calculate summary
    let pg_total: f64 = all_results
        .iter()
        .filter(|r| r.engine == "PostGIS")
        .map(|r| r.duration_ms)
        .sum();
    let moby_total: f64 = all_results
        .iter()
        .filter(|r| r.engine == "MobyDB")
        .map(|r| r.duration_ms)
        .sum();

    let dataset_size: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM breadcrumbs")
        .fetch_one(&state.pg)
        .await
        .unwrap_or((0,));

    let suite = BenchmarkSuite {
        run_id,
        started_at,
        dataset_size: dataset_size.0,
        results: all_results,
        summary: BenchmarkSummary {
            postgis_total_ms: pg_total,
            mobydb_total_ms: moby_total,
            speedup_factor: if moby_total > 0.0 {
                pg_total / moby_total
            } else {
                0.0
            },
        },
    };

    info!(
        "Benchmark complete. PostGIS: {:.1}ms, MobyDB: {:.1}ms, Speedup: {:.2}x",
        suite.summary.postgis_total_ms,
        suite.summary.mobydb_total_ms,
        suite.summary.speedup_factor
    );

    Json(suite)
}

// ── Latest results (placeholder) ───────────────────────────

async fn get_latest_results() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "message": "Run POST /api/benchmark/full to generate results"
    }))
}

// ── Data generation ────────────────────────────────────────

fn generate_breadcrumbs(params: &SeedParams) -> Vec<Breadcrumb> {
    use h3o::{LatLng, Resolution};
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let mut breadcrumbs = Vec::new();

    for entity_idx in 0..params.entities {
        // Generate an Ed25519 keypair for this entity
        let signing_key = ed25519_dalek::SigningKey::generate(&mut rng);
        let pubkey_hex = hex::encode(signing_key.verifying_key().as_bytes());
        let signing_key_hex = hex::encode(signing_key.to_bytes());

        // Start position near center with some jitter
        let mut lat = params.center_lat + rng.gen_range(-0.05..0.05);
        let mut lng = params.center_lng + rng.gen_range(-0.05..0.05);

        for point_idx in 0..params.points_per_entity {
            // Simulate movement: random walk
            lat += rng.gen_range(-0.001..0.001);
            lng += rng.gen_range(-0.001..0.001);

            // Convert to H3
            let ll = LatLng::new(lat, lng).expect("valid latlng");
            let cell = ll.to_cell(Resolution::Seven);
            let h3_str = cell.to_string();
            let h3_u64 = u64::from(cell);

            // GEP epoch (incrementing)
            let epoch = (entity_idx * params.points_per_entity + point_idx) as i64 + 1;

            breadcrumbs.push(Breadcrumb {
                public_key: pubkey_hex.clone(),
                h3_cell: h3_str,
                h3_cell_u64: h3_u64,
                epoch,
                latitude: lat,
                longitude: lng,
                altitude_m: rng.gen_range(0.0..100.0),
                speed_mps: rng.gen_range(0.0..30.0),
                payload: serde_json::json!({
                    "entity": entity_idx,
                    "seq": point_idx,
                    "type": if entity_idx % 3 == 0 { "vehicle" }
                           else if entity_idx % 3 == 1 { "drone" }
                           else { "pedestrian" },
                }),
                signing_key_hex: signing_key_hex.clone(),
            });
        }
    }

    breadcrumbs
}
