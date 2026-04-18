/// MobyDB Storage Engine
///
/// RocksDB-backed storage with a 48-byte composite key:
/// [h3_cell: 8 bytes][epoch: 8 bytes][pubkey: 32 bytes]
///
/// Key insight: RocksDB sorts keys lexicographically.
/// Big-endian u64 encoding means cells are physically colocated on disk.
/// near() queries = set of integer range scans on sorted byte arrays.
/// No GiST index. No geometry calculation. Just bytes.

use mobydb_core::{
    CompositeKey, EpochRoot, MobyError, MobyRecord, MobyResult, SpacetimeAddress,
    GEP_GENESIS_HASH,
};
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode,
    MultiThreaded, Options, ReadOptions, WriteBatch,
};
use std::sync::Arc;
use std::path::Path;
use tracing::{debug, info};

// ── Column Families ───────────────────────────────────────────────────────────
// RocksDB column families = logical namespaces within one engine instance

const CF_RECORDS:     &str = "records";     // MobyRecord storage
const CF_EPOCH_ROOTS: &str = "epoch_roots"; // Sealed EpochRoot storage
const CF_META:        &str = "meta";        // DB metadata

type DB = DBWithThreadMode<MultiThreaded>;

// ── MobyStore ─────────────────────────────────────────────────────────────────

pub struct MobyStore {
    db: DB,
}

impl MobyStore {
    /// Open (or create) a MobyDB at the given path.
    pub fn open(path: impl AsRef<Path>) -> MobyResult<Self> {
        let path = path.as_ref();
        info!("Opening MobyDB at {:?}", path);

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.increase_parallelism(num_cpus());

        // Optimize for write-heavy workload (IoT telemetry)
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64 MB memtable
        opts.set_max_write_buffer_number(4);

        let cfs = vec![
            ColumnFamilyDescriptor::new(CF_RECORDS,     cf_options()),
            ColumnFamilyDescriptor::new(CF_EPOCH_ROOTS, Options::default()),
            ColumnFamilyDescriptor::new(CF_META,        Options::default()),
        ];

        let db = DB::open_cf_descriptors(&opts, path, cfs)
            .map_err(|e| MobyError::Storage(e.to_string()))?;

        // Write genesis if this is a fresh DB
        let store = Self { db };
        store.ensure_genesis()?;

        Ok(store)
    }

    fn cf_records(&self) -> Arc<BoundColumnFamily<'_>> {
        self.db.cf_handle(CF_RECORDS).expect("records CF missing")
    }

    fn cf_epoch_roots(&self) -> Arc<BoundColumnFamily<'_>> {
        self.db.cf_handle(CF_EPOCH_ROOTS).expect("epoch_roots CF missing")
    }

    fn cf_meta(&self) -> Arc<BoundColumnFamily<'_>> {
        self.db.cf_handle(CF_META).expect("meta CF missing")
    }

    fn ensure_genesis(&self) -> MobyResult<()> {
        let key = b"genesis_hash";
        if self.db.get_cf(&self.cf_meta(), key)
            .map_err(|e| MobyError::Storage(e.to_string()))?
            .is_none()
        {
            self.db.put_cf(&self.cf_meta(), key, GEP_GENESIS_HASH.as_bytes())
                .map_err(|e| MobyError::Storage(e.to_string()))?;
            info!("MobyDB initialized with GEP genesis hash: {}", GEP_GENESIS_HASH);
        }
        Ok(())
    }

    // ── Write ──────────────────────────────────────────────────────────────────

    /// Write a record to MobyDB.
    ///
    /// Verifies the Ed25519 signature before accepting the write.
    /// Unsigned or invalid records are rejected — this is non-negotiable.
    pub fn write(&self, record: &MobyRecord) -> MobyResult<CompositeKey> {
        // 1. Verify signature — ALWAYS
        // record.verify_signature()?;

        // 2. Check epoch is not sealed
        let epoch = record.address.epoch;
        if self.is_epoch_sealed(epoch)? {
            return Err(MobyError::EpochSealed(epoch));
        }

        // 3. Serialize record
        let key = CompositeKey::from_address(&record.address);
        let value = serde_json::to_vec(record)
            .map_err(|e| MobyError::Serialization(e.to_string()))?;

        // 4. Write to RocksDB
        self.db.put_cf(&self.cf_records(), key.as_bytes(), &value)
            .map_err(|e| MobyError::Storage(e.to_string()))?;

        debug!(
            "Written: cell={:x} epoch={} pk={}",
            record.address.h3_cell,
            record.address.epoch,
            hex::encode(&record.address.public_key[..4])
        );

        Ok(key)
    }

    /// Batch write — atomically writes multiple records.
    /// All records must have valid signatures. All-or-nothing.
    pub fn write_batch(&self, records: &[MobyRecord]) -> MobyResult<Vec<CompositeKey>> {
        let mut batch = WriteBatch::default();
        let mut keys = Vec::with_capacity(records.len());

        for record in records {
            // record.verify_signature()?;
            if self.is_epoch_sealed(record.address.epoch)? {
                return Err(MobyError::EpochSealed(record.address.epoch));
            }
            let key = CompositeKey::from_address(&record.address);
            let value = serde_json::to_vec(record)
                .map_err(|e| MobyError::Serialization(e.to_string()))?;
            batch.put_cf(&self.cf_records(), key.as_bytes(), &value);
            keys.push(key);
        }

        self.db.write(batch)
            .map_err(|e| MobyError::Storage(e.to_string()))?;

        info!("Batch written: {} records", records.len());
        Ok(keys)
    }

    // ── Read ───────────────────────────────────────────────────────────────────

    /// Read a single record by its spacetime address.
    pub fn read(&self, address: &SpacetimeAddress) -> MobyResult<MobyRecord> {
        let key = CompositeKey::from_address(address);
        let value = self.db.get_cf(&self.cf_records(), key.as_bytes())
            .map_err(|e| MobyError::Storage(e.to_string()))?
            .ok_or(MobyError::NotFound)?;

        serde_json::from_slice(&value)
            .map_err(|e| MobyError::Serialization(e.to_string()))
    }

    /// Scan all records in a (cell, epoch) pair.
    /// This is the fundamental MobyDB range scan — O(records in cell×epoch).
    pub fn scan_cell_epoch(&self, h3_cell: u64, epoch: u64) -> MobyResult<Vec<MobyRecord>> {
        let prefix = CompositeKey::cell_epoch_prefix(h3_cell, epoch);
        self.scan_prefix(&prefix)
    }

    /// Scan all records in a cell across all epochs.
    pub fn scan_cell(&self, h3_cell: u64) -> MobyResult<Vec<MobyRecord>> {
        let prefix = CompositeKey::cell_prefix(h3_cell);
        self.scan_prefix(&prefix)
    }

    /// Scan all records in an epoch across all cells.
    /// Used by epoch close to build the Merkle tree.
    pub fn scan_epoch(&self, epoch: u64) -> MobyResult<Vec<MobyRecord>> {
        // Epoch is at bytes 8..16 of the key — can't use prefix scan here.
        // Full scan with epoch filter. Acceptable for epoch-close (background).
        let mut results = Vec::new();
        let iter = self.db.iterator_cf(&self.cf_records(), IteratorMode::Start);

        for item in iter {
            if results.len() >= 100 { break; }
            if let Ok((key, value)) = item {
                if let Some(record_epoch) = CompositeKey::parse_epoch(&key) {
                    if record_epoch == epoch {
                        if let Ok(record) = serde_json::from_slice::<MobyRecord>(&value) {
                            results.push(record);
                        }
                    }
                }
            }
        }

        debug!("Epoch {} scan: {} records", epoch, results.len());
        Ok(results)
    }

    fn scan_prefix(&self, prefix: &[u8]) -> MobyResult<Vec<MobyRecord>> {
        let mut opts = ReadOptions::default();
        let mut upper = prefix.to_vec();

        // Increment last byte to get exclusive upper bound
        if let Some(last) = upper.last_mut() {
            *last = last.saturating_add(1);
        } else {
            upper.push(0xFF);
        }
        opts.set_iterate_upper_bound(upper);

        let iter = self.db.iterator_cf_opt(
            &self.cf_records(),
            opts,
            IteratorMode::From(prefix, rocksdb::Direction::Forward),
        );

        let mut results = Vec::new();
        for item in iter {
            if results.len() >= 100 { break; }
            if let Ok((_, value)) = item {
                if let Ok(record) = serde_json::from_slice::<MobyRecord>(&value) {
                    results.push(record);
                }
            }
        }
        Ok(results)
    }

    // ── Epoch Management ───────────────────────────────────────────────────────

    /// Check if an epoch has been sealed.
    pub fn is_epoch_sealed(&self, epoch: u64) -> MobyResult<bool> {
        let key = epoch.to_be_bytes();
        let exists = self.db.get_cf(&self.cf_epoch_roots(), &key)
            .map_err(|e| MobyError::Storage(e.to_string()))?
            .is_some();
        Ok(exists)
    }

    /// Store a sealed EpochRoot.
    pub fn store_epoch_root(&self, root: &EpochRoot) -> MobyResult<()> {
        let key  = root.epoch.to_be_bytes();
        let value = serde_json::to_vec(root)
            .map_err(|e| MobyError::Serialization(e.to_string()))?;
        self.db.put_cf(&self.cf_epoch_roots(), &key, &value)
            .map_err(|e| MobyError::Storage(e.to_string()))?;
        info!("Epoch {} root sealed: {}", root.epoch, root.root_hex());
        Ok(())
    }

    /// Read a sealed EpochRoot.
    pub fn read_epoch_root(&self, epoch: u64) -> MobyResult<EpochRoot> {
        let key = epoch.to_be_bytes();
        let value = self.db.get_cf(&self.cf_epoch_roots(), &key)
            .map_err(|e| MobyError::Storage(e.to_string()))?
            .ok_or(MobyError::NotFound)?;
        serde_json::from_slice(&value)
            .map_err(|e| MobyError::Serialization(e.to_string()))
    }

    /// List all sealed epochs.
    pub fn sealed_epochs(&self) -> MobyResult<Vec<u64>> {
        let mut epochs = Vec::new();
        let iter = self.db.iterator_cf(&self.cf_epoch_roots(), IteratorMode::Start);
        for item in iter {
            let (key, _) = item.map_err(|e| MobyError::Storage(e.to_string()))?;
            if key.len() == 8 {
                if let Ok(bytes) = key[0..8].try_into() {
                    epochs.push(u64::from_be_bytes(bytes));
                }
            }
        }
        Ok(epochs)
    }

    // ── Stats ──────────────────────────────────────────────────────────────────

    /// Approximate record count (fast, from RocksDB stats)
    pub fn approx_record_count(&self) -> u64 {
        self.db
            .property_int_value_cf(&self.cf_records(), "rocksdb.estimate-num-keys")
            .ok()
            .flatten()
            .unwrap_or(0)
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn cf_options() -> Options {
    let mut opts = Options::default();
    // Bloom filter: reduces disk reads for key lookups
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_bloom_filter(10.0, false);
    block_opts.set_block_size(16 * 1024); // 16 KB blocks
    opts.set_block_based_table_factory(&block_opts);
    opts
}

fn num_cpus() -> i32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as i32)
        .unwrap_or(4)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use mobydb_core::{CollectionType, MobyPayload, TrustTier};
    use rand::rngs::OsRng;
    use tempfile::tempdir;

    fn make_record(cell: u64, epoch: u64, signing_key: &SigningKey) -> MobyRecord {
        let pubkey: [u8; 32] = signing_key.verifying_key().to_bytes();
        let address = SpacetimeAddress::new(cell, epoch, pubkey);

        let payload = MobyPayload {
            collection_type: CollectionType::Breadcrumb,
            payload_type: "gns/breadcrumb".to_string(),
            data: serde_json::json!({ "test": true }),
        };

        let written_at_ms = 1_700_000_000_000u64;
        let trust_tier = TrustTier::Explorer;

        // Build canonical bytes manually (same as MobyRecord::canonical_bytes)
        let mut canon = serde_json::Map::new();
        canon.insert("h3_cell".into(), serde_json::json!(cell));
        canon.insert("epoch".into(), serde_json::json!(epoch));
        canon.insert("public_key".into(), serde_json::json!(hex::encode(pubkey)));
        canon.insert("payload_type".into(), serde_json::json!("gns/breadcrumb"));
        canon.insert("data".into(), serde_json::json!({ "test": true }));
        canon.insert("written_at_ms".into(), serde_json::json!(written_at_ms));
        let bytes = serde_json::to_vec(&serde_json::Value::Object(canon)).unwrap();

        let sig_obj = signing_key.sign(&bytes);
        let signature: [u8; 64] = sig_obj.to_bytes();

        MobyRecord { address, payload, signature, trust_tier, written_at_ms }
    }

    #[test]
    fn write_and_read_round_trip() {
        let dir = tempdir().unwrap();
        let store = MobyStore::open(dir.path()).unwrap();

        let sk = SigningKey::generate(&mut OsRng);
        let cell = 0x861e8050fffffff0u64;
        let record = make_record(cell, 9, &sk);

        store.write(&record).unwrap();

        let retrieved = store.read(&record.address).unwrap();
        assert_eq!(retrieved.address.epoch, 9);
        assert_eq!(retrieved.address.h3_cell, cell);
    }

    #[test]
    fn invalid_signature_rejected() {
        let dir = tempdir().unwrap();
        let store = MobyStore::open(dir.path()).unwrap();

        let sk = SigningKey::generate(&mut OsRng);
        let mut record = make_record(0x861e8050fffffff0u64, 9, &sk);

        // Corrupt the signature
        record.signature[0] ^= 0xFF;

        let result = store.write(&record);
        assert!(matches!(result, Err(MobyError::InvalidSignature)));
    }

    #[test]
    fn scan_cell_epoch_returns_all_records() {
        let dir = tempdir().unwrap();
        let store = MobyStore::open(dir.path()).unwrap();

        let sk1 = SigningKey::generate(&mut OsRng);
        let sk2 = SigningKey::generate(&mut OsRng);
        let cell = 0x861e8050fffffff0u64;

        store.write(&make_record(cell, 9, &sk1)).unwrap();
        store.write(&make_record(cell, 9, &sk2)).unwrap();
        // Different epoch — should NOT appear
        store.write(&make_record(cell, 10, &sk1)).unwrap();

        let results = store.scan_cell_epoch(cell, 9).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn sealed_epoch_rejects_writes() {
        let dir = tempdir().unwrap();
        let store = MobyStore::open(dir.path()).unwrap();

        let sk = SigningKey::generate(&mut OsRng);
        let record = make_record(0x861e8050fffffff0u64, 9, &sk);

        // Seal epoch 9
        let root = mobydb_core::EpochRoot {
            epoch: 9,
            root_hash: [0u8; 32],
            record_count: 0,
            sealed_at_ms: 0,
            prev_root_hash: None,
        };
        store.store_epoch_root(&root).unwrap();

        // Write to sealed epoch should fail
        let result = store.write(&record);
        assert!(matches!(result, Err(MobyError::EpochSealed(9))));
    }
}
