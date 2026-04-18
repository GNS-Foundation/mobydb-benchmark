/// MobyDB Query Engine — MobyQL
///
/// The three fundamental MobyDB query operators:
///
///   near(origin, rings)   — proximity: all H3 cells within N rings
///   during(start, end)    — temporal: epoch range scan
///   zoom_out(resolution)  — aggregation: H3 resolution rollup
///
/// These are not SQL functions. They are spatial-temporal primitives
/// that compile directly to integer range scans on the storage engine.

use h3o::{CellIndex, Resolution};
use mobydb_core::{CollectionType, MobyError, MobyRecord, MobyResult, TrustTier};
use mobydb_storage::MobyStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::debug;

// ── Query Builder ─────────────────────────────────────────────────────────────

/// A MobyDB query — built via method chaining.
///
/// ```ignore
/// let results = MobyQuery::near(cell_id, 2)
///     .during(8, 12)
///     .with_tier(TrustTier::Explorer)
///     .execute(&store)?;
/// ```
pub struct MobyQuery {
    origin_cell:     u64,
    rings:           u32,
    epoch_start:     u64,
    epoch_end:       u64,
    min_tier:        Option<TrustTier>,
    collection_type: Option<CollectionType>,
    limit:           Option<usize>,
}

impl MobyQuery {
    /// Start a proximity query: find records within N hexagonal rings of origin.
    ///
    /// `rings: 0` → origin cell only (1 cell)
    /// `rings: 1` → origin + 6 neighbors (7 cells)
    /// `rings: 2` → 19 cells, `rings: 3` → 37 cells
    pub fn near(origin_cell: u64, rings: u32) -> Self {
        Self {
            origin_cell,
            rings,
            epoch_start:     0,
            epoch_end:       u64::MAX,
            min_tier:        None,
            collection_type: None,
            limit:           None,
        }
    }

    /// Filter by epoch range (inclusive on both ends).
    pub fn during(mut self, epoch_start: u64, epoch_end: u64) -> Self {
        self.epoch_start = epoch_start;
        self.epoch_end   = epoch_end;
        self
    }

    /// Filter: only records with trust_tier >= tier.
    pub fn with_tier(mut self, tier: TrustTier) -> Self {
        self.min_tier = Some(tier);
        self
    }

    /// Filter by collection type.
    pub fn collection(mut self, ct: CollectionType) -> Self {
        self.collection_type = Some(ct);
        self
    }

    /// Limit number of results returned.
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Execute the query against a MobyStore.
    pub fn execute(self, store: &MobyStore) -> MobyResult<QueryResult> {
        // 1. Expand origin to all cells within N rings
        let cells = expand_rings(self.origin_cell, self.rings)?;
        debug!(
            "MobyQL near(): {} cells, epochs {}..{}",
            cells.len(), self.epoch_start, self.epoch_end
        );

        // 2. For each (cell, epoch) pair, range scan
        let mut records: Vec<MobyRecord> = Vec::new();
        let epochs: Vec<u64> = (self.epoch_start..=self.epoch_end).collect();

        for cell in &cells {
            for epoch in &epochs {
                let mut batch = store.scan_cell_epoch(*cell, *epoch)?;
                records.append(&mut batch);
            }
        }

        // 3. Apply identity and collection filters
        let mut filtered: Vec<MobyRecord> = records
            .into_iter()
            .filter(|r| {
                if let Some(min) = &self.min_tier {
                    if r.trust_tier < *min { return false; }
                }
                if let Some(ct) = &self.collection_type {
                    if &r.payload.collection_type != ct { return false; }
                }
                true
            })
            .collect();

        // 4. Sort: most recent epoch first, then by cell
        filtered.sort_by(|a, b| {
            b.address.epoch.cmp(&a.address.epoch)
                .then(a.address.h3_cell.cmp(&b.address.h3_cell))
        });

        // 5. Limit
        if let Some(n) = self.limit {
            filtered.truncate(n);
        }

        let count         = filtered.len();
        let epochs_scanned = epochs.len();
        Ok(QueryResult {
            records:       filtered,
            cells_scanned: cells.len(),
            epochs_scanned,
            count,
        })
    }
}

// ── Zoom Out (Resolution-Native Aggregation) ──────────────────────────────────

/// Aggregate records from fine resolution up to a coarser target resolution.
///
/// This is the GROUP BY killer: `zoom_out(resolution: 5)` groups all
/// Res-9 records by their Res-5 parent automatically — no JOIN,
/// no boundary table, no materialized view.
pub struct ZoomQuery {
    source_cells:      Vec<u64>,
    source_epoch:      u64,
    target_resolution: u8,
    aggregation:       AggregationType,
    value_path:        String,
}

#[derive(Debug, Clone)]
pub enum AggregationType {
    Count,
    Sum,
    Average,
    Max,
    Min,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ZoomResult {
    /// Parent cell at the target resolution
    pub parent_cell: u64,
    /// Number of child records aggregated
    pub count:       u64,
    /// Aggregated value (None for Count aggregation)
    pub value:       Option<f64>,
    /// Source epoch
    pub epoch:       u64,
}

impl ZoomQuery {
    pub fn new(
        source_cells:      Vec<u64>,
        epoch:             u64,
        target_resolution: u8,
        aggregation:       AggregationType,
        value_path:        String,
    ) -> Self {
        Self { source_cells, source_epoch: epoch, target_resolution, aggregation, value_path }
    }

    pub fn execute(self, store: &MobyStore) -> MobyResult<Vec<ZoomResult>> {
        let target_res = Resolution::try_from(self.target_resolution)
            .map_err(|_| MobyError::InvalidCell(
                format!("invalid resolution: {}", self.target_resolution)
            ))?;

        // Group values by parent cell at target resolution
        let mut groups: HashMap<u64, Vec<f64>> = HashMap::new();

        for cell_id in &self.source_cells {
            let records = store.scan_cell_epoch(*cell_id, self.source_epoch)?;

            for record in records {
                // Walk up to parent at target resolution — O(1) in h3o
                let cell = CellIndex::try_from(record.address.h3_cell)
                    .map_err(|e| MobyError::InvalidCell(e.to_string()))?;

                let parent = cell.parent(target_res).ok_or_else(|| {
                    MobyError::InvalidCell(format!(
                        "cell {:x} has no parent at resolution {}",
                        record.address.h3_cell, self.target_resolution
                    ))
                })?;

                let parent_id = u64::from(parent);
                let val = extract_value(&record.payload.data, &self.value_path)
                    .unwrap_or(1.0); // fallback: count mode
                groups.entry(parent_id).or_default().push(val);
            }
        }

        // Aggregate each group
        let results = groups
            .into_iter()
            .map(|(parent_cell, values)| {
                let count = values.len() as u64;
                let value = match &self.aggregation {
                    AggregationType::Count   => None,
                    AggregationType::Sum     => Some(values.iter().sum()),
                    AggregationType::Average => Some(values.iter().sum::<f64>() / count as f64),
                    AggregationType::Max     => values.iter().cloned().reduce(f64::max),
                    AggregationType::Min     => values.iter().cloned().reduce(f64::min),
                };
                ZoomResult { parent_cell, count, value, epoch: self.source_epoch }
            })
            .collect();

        Ok(results)
    }
}

// ── Query Result ──────────────────────────────────────────────────────────────

pub struct QueryResult {
    pub records:        Vec<MobyRecord>,
    pub cells_scanned:  usize,
    pub epochs_scanned: usize,
    pub count:          usize,
}

impl QueryResult {
    pub fn is_empty(&self) -> bool { self.records.is_empty() }
}

// ── Core Primitive: expand_rings ─────────────────────────────────────────────

/// Expand an H3 cell to all cells within N hexagonal rings.
/// Returns a Vec of H3 cell IDs as u64 integers.
///
/// Pure Rust via h3o — no C FFI, no CGO, no bindings.
/// Ring expansion is the core of the near() operator.
pub fn expand_rings(origin: u64, rings: u32) -> MobyResult<Vec<u64>> {
    let cell = CellIndex::try_from(origin)
        .map_err(|e| MobyError::InvalidCell(e.to_string()))?;

    // Sanitize the radius — cap at 10 to prevent runaway expansion
    let safe_rings = rings.min(10);

    if safe_rings == 0 {
        return Ok(vec![origin]);
    }

    // grid_disk_safe returns origin + all cells within N rings
    // Manual loop to avoid .collect() capacity overflow from broken size hints
    let max_cells = 500; // rings=10 → 331 cells max; 500 is a safe ceiling
    let mut all_cells = Vec::with_capacity(max_cells);
    for ci in cell.grid_disk_safe(safe_rings) {
        if all_cells.len() >= max_cells { break; }
        all_cells.push(u64::from(ci));
    }

    debug!("expand_rings({}, {}): {} cells", origin, rings, all_cells.len());
    Ok(all_cells)
}

/// Extract a numeric value from a JSON payload using dot-notation path.
/// "voltage.delta" on {"voltage": {"delta": 12.4}} → Some(12.4)
fn extract_value(data: &serde_json::Value, path: &str) -> Option<f64> {
    let mut current = data;
    for key in path.split('.') {
        current = current.get(key)?;
    }
    current.as_f64()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use h3o::LatLng;

    fn rome_cell() -> u64 {
        let latlng = LatLng::new(41.9028, 12.4964).unwrap();
        u64::from(latlng.to_cell(Resolution::Seven))
    }

    #[test]
    fn expand_rings_zero_returns_one_cell() {
        let cells = expand_rings(rome_cell(), 0).unwrap();
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0], rome_cell());
    }

    #[test]
    fn expand_rings_one_returns_seven_cells() {
        let cells = expand_rings(rome_cell(), 1).unwrap();
        // Origin + 6 equidistant neighbors
        assert_eq!(cells.len(), 7);
    }

    #[test]
    fn expand_rings_two_returns_nineteen_cells() {
        let cells = expand_rings(rome_cell(), 2).unwrap();
        // 1 + 6 + 12 = 19
        assert_eq!(cells.len(), 19);
    }

    #[test]
    fn expand_rings_three_returns_thirtyseven_cells() {
        let cells = expand_rings(rome_cell(), 3).unwrap();
        // 1 + 6 + 12 + 18 = 37
        assert_eq!(cells.len(), 37);
    }

    #[test]
    fn extract_nested_value_works() {
        let data = serde_json::json!({"sensor": {"voltage": {"delta": 12.4}}});
        assert_eq!(extract_value(&data, "sensor.voltage.delta"), Some(12.4));
    }

    #[test]
    fn extract_missing_path_returns_none() {
        let data = serde_json::json!({"sensor": {}});
        assert_eq!(extract_value(&data, "sensor.missing.key"), None);
    }
}
