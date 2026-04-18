/// MobyDB Core — Spacetime Identity Types
///
/// The fundamental data model: every record has a three-dimensional
/// address in (H3 cell, GEP epoch, Ed25519 public key) space.
/// Geography is WHERE. Epoch is WHEN. Public key is WHO.

use ed25519_dalek::{Signature, VerifyingKey};
use h3o::{CellIndex, Resolution};
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum MobyError {
    #[error("Invalid signature: record rejected")]
    InvalidSignature,

    #[error("Invalid H3 cell index: {0}")]
    InvalidCell(String),

    #[error("Invalid public key: {0}")]
    InvalidPublicKey(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Epoch already sealed: {0}")]
    EpochSealed(u64),

    #[error("Record not found")]
    NotFound,
}

pub type MobyResult<T> = Result<T, MobyError>;

// ── Trust Tier ────────────────────────────────────────────────────────────────

/// GNS Trust Tier — earned through Proof-of-Trajectory.
/// IoT devices receive Certified (separate ladder from humans).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
pub enum TrustTier {
    Unknown     = 0,
    Seedling    = 1,
    Explorer    = 2,
    Navigator   = 3,
    Trailblazer = 4,
    Sovereign   = 5,
    /// IoT devices: provisioned and org-certified
    Certified   = 10,
}

impl std::fmt::Display for TrustTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown     => write!(f, "unknown"),
            Self::Seedling    => write!(f, "seedling"),
            Self::Explorer    => write!(f, "explorer"),
            Self::Navigator   => write!(f, "navigator"),
            Self::Trailblazer => write!(f, "trailblazer"),
            Self::Sovereign   => write!(f, "sovereign"),
            Self::Certified   => write!(f, "certified"),
        }
    }
}

// ── Collection Type ───────────────────────────────────────────────────────────

/// The six native MobyDB collection types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectionType {
    /// Trajectory point: one record per entity per H3 cell per epoch
    Breadcrumb,
    /// Time-series sensor reading: high-frequency, small payload
    Telemetry,
    /// Discrete occurrence: incident, transaction, alert
    Event,
    /// H3 cell set defining a geographic region
    Territory,
    /// Edge between two spacetime addresses
    Relationship,
    /// Pre-computed rollup at lower resolution
    Aggregate,
    /// Georeferenced imagery tile: satellite, aerial, drone capture
    Imagery,
    /// AI inference result: model, prompt hash, response hash, latency
    Inference,
}

// ── Spacetime Address ─────────────────────────────────────────────────────────

/// The primary key of MobyDB.
/// WHERE = h3_cell (64-bit H3 index)
/// WHEN  = epoch   (GEP epoch number)
/// WHO   = pubkey  (Ed25519 public key, 32 bytes)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpacetimeAddress {
    /// H3 cell index (64-bit integer, resolution 0–15)
    pub h3_cell: u64,
    /// GEP epoch number (monotonically increasing)
    pub epoch: u64,
    /// Ed25519 public key (32 bytes, hex-encoded in JSON)
    #[serde(
        serialize_with   = "serialize_pubkey",
        deserialize_with = "deserialize_pubkey"
    )]
    pub public_key: [u8; 32],
}

impl SpacetimeAddress {
    pub fn new(h3_cell: u64, epoch: u64, public_key: [u8; 32]) -> Self {
        Self { h3_cell, epoch, public_key }
    }

    /// Derive from lat/lng at a given resolution
    pub fn from_latlng(lat: f64, lng: f64, resolution: u8, epoch: u64, public_key: [u8; 32]) -> MobyResult<Self> {
        let res = Resolution::try_from(resolution)
            .map_err(|_| MobyError::InvalidCell(format!("invalid resolution: {}", resolution)))?;
        let latlng = h3o::LatLng::new(lat, lng)
            .map_err(|e| MobyError::InvalidCell(e.to_string()))?;
        let cell = latlng.to_cell(res);
        Ok(Self::new(u64::from(cell), epoch, public_key))
    }

    /// Get the H3 CellIndex
    pub fn cell_index(&self) -> MobyResult<CellIndex> {
        CellIndex::try_from(self.h3_cell)
            .map_err(|e| MobyError::InvalidCell(e.to_string()))
    }

    /// Get resolution of this cell
    pub fn resolution(&self) -> MobyResult<u8> {
        Ok(u8::from(self.cell_index()?.resolution()))
    }
}

fn serialize_pubkey<S: serde::Serializer>(key: &[u8; 32], s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&hex::encode(key))
}

fn deserialize_pubkey<'de, D: serde::Deserializer<'de>>(d: D) -> Result<[u8; 32], D::Error> {
    let s = String::deserialize(d)?;
    let bytes = hex::decode(&s).map_err(serde::de::Error::custom)?;
    bytes.try_into().map_err(|_| serde::de::Error::custom("pubkey must be 32 bytes"))
}

// ── Composite Key ─────────────────────────────────────────────────────────────

/// 48-byte RocksDB composite key.
/// Serialized as big-endian bytes so RocksDB's lexicographic sort
/// naturally orders by (cell, epoch, pubkey).
///
/// Layout: [h3_cell: 8][epoch: 8][pubkey: 32]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CompositeKey([u8; 48]);

impl CompositeKey {
    pub fn new(h3_cell: u64, epoch: u64, pubkey: &[u8; 32]) -> Self {
        let mut key = [0u8; 48];
        key[0..8].copy_from_slice(&h3_cell.to_be_bytes());
        key[8..16].copy_from_slice(&epoch.to_be_bytes());
        key[16..48].copy_from_slice(pubkey);
        Self(key)
    }

    pub fn from_address(addr: &SpacetimeAddress) -> Self {
        Self::new(addr.h3_cell, addr.epoch, &addr.public_key)
    }

    /// Prefix key for scanning all records in a (cell, epoch) pair
    pub fn cell_epoch_prefix(h3_cell: u64, epoch: u64) -> [u8; 16] {
        let mut prefix = [0u8; 16];
        prefix[0..8].copy_from_slice(&h3_cell.to_be_bytes());
        prefix[8..16].copy_from_slice(&epoch.to_be_bytes());
        prefix
    }

    /// Prefix key for scanning all records in a cell (any epoch)
    pub fn cell_prefix(h3_cell: u64) -> [u8; 8] {
        h3_cell.to_be_bytes()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    /// Parse h3_cell from raw key bytes
    pub fn parse_cell(bytes: &[u8]) -> Option<u64> {
        if bytes.len() >= 8 {
            Some(u64::from_be_bytes(bytes[0..8].try_into().ok()?))
        } else {
            None
        }
    }

    /// Parse epoch from raw key bytes  
    pub fn parse_epoch(bytes: &[u8]) -> Option<u64> {
        if bytes.len() >= 16 {
            Some(u64::from_be_bytes(bytes[8..16].try_into().ok()?))
        } else {
            None
        }
    }
}

// ── Payload ───────────────────────────────────────────────────────────────────

/// Flexible payload — any JSON-serializable data.
/// The collection_type determines how it is indexed and queried.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MobyPayload {
    pub collection_type: CollectionType,
    /// Payload type string (e.g. "gns/breadcrumb", "telemetry/voltage")
    pub payload_type: String,
    /// The actual data — schema-flexible
    pub data: serde_json::Value,
}

// ── Imagery Reference ─────────────────────────────────────────────────────────

/// Reference to an image tile stored externally.
///
/// MobyDB does not store raw pixel data — it stores signed metadata
/// and a content hash. The hash is covered by the Ed25519 signature,
/// creating a tamper-evident chain from capture to query.
///
/// Storage backends: Cloudflare R2, S3, local filesystem, IPFS.
///
/// ```ignore
/// let imagery = ImageryRef::new(
///     "sentinel2/B04",
///     tile_bytes,                     // raw bytes — hashed, not stored
///     "r2://mobydb-imagery/scene123/tile_871e9a0ec.webp",
/// )
/// .with_band("B04")
/// .with_resolution_m(10.0)
/// .with_cloud_cover(12.4);
///
/// let payload = imagery.to_payload();
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageryRef {
    /// Blake3 hash of the raw image bytes (hex-encoded)
    pub content_hash: String,
    /// URL or path to the actual image bytes
    pub content_url: String,
    /// Size of the image in bytes
    pub content_size: u64,
    /// Image format: "webp", "png", "tiff", "cog"
    pub format: String,
    /// Spectral band: "RGB", "B04", "NDVI", "thermal"
    pub band: Option<String>,
    /// Ground sample distance in metres
    pub resolution_m: Option<f64>,
    /// Cloud cover percentage (0.0–100.0)
    pub cloud_cover_pct: Option<f64>,
    /// Source sensor/platform: "sentinel2", "landsat9", "drone-dji-m30t"
    pub source: Option<String>,
    /// H3 resolution used for tiling
    pub tile_resolution: Option<u8>,
}

impl ImageryRef {
    /// Create a new imagery reference from raw bytes.
    /// Computes the Blake3 hash but does NOT store the bytes.
    pub fn new(format: &str, raw_bytes: &[u8], content_url: &str) -> Self {
        let hash = blake3::hash(raw_bytes);
        Self {
            content_hash: hash.to_hex().to_string(),
            content_url: content_url.to_string(),
            content_size: raw_bytes.len() as u64,
            format: format.to_string(),
            band: None,
            resolution_m: None,
            cloud_cover_pct: None,
            source: None,
            tile_resolution: None,
        }
    }

    /// Create from a pre-computed hash (when bytes aren't available locally)
    pub fn from_hash(format: &str, content_hash: &str, content_url: &str, content_size: u64) -> Self {
        Self {
            content_hash: content_hash.to_string(),
            content_url: content_url.to_string(),
            content_size,
            format: format.to_string(),
            band: None,
            resolution_m: None,
            cloud_cover_pct: None,
            source: None,
            tile_resolution: None,
        }
    }

    pub fn with_band(mut self, band: &str) -> Self { self.band = Some(band.into()); self }
    pub fn with_resolution_m(mut self, res: f64) -> Self { self.resolution_m = Some(res); self }
    pub fn with_cloud_cover(mut self, pct: f64) -> Self { self.cloud_cover_pct = Some(pct); self }
    pub fn with_source(mut self, src: &str) -> Self { self.source = Some(src.into()); self }
    pub fn with_tile_resolution(mut self, res: u8) -> Self { self.tile_resolution = Some(res); self }

    /// Convert to a MobyPayload ready for record construction
    pub fn to_payload(&self) -> MobyPayload {
        MobyPayload {
            collection_type: CollectionType::Imagery,
            payload_type: format!("imagery/{}", self.source.as_deref().unwrap_or("unknown")),
            data: serde_json::to_value(self).unwrap_or_default(),
        }
    }

    /// Verify that external bytes match the stored hash
    pub fn verify_content(&self, bytes: &[u8]) -> bool {
        let hash = blake3::hash(bytes);
        hash.to_hex().to_string() == self.content_hash
    }
}

// ── Record ────────────────────────────────────────────────────────────────────

/// A MobyDB record — the fundamental unit of storage.
/// Every record is signed by the entity at the spacetime address.
/// Unsigned records are rejected at write time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MobyRecord {
    /// The three-dimensional spacetime address (primary key)
    pub address: SpacetimeAddress,
    /// The payload
    pub payload: MobyPayload,
    /// Ed25519 signature over canonical_bytes()
    #[serde(
        serialize_with   = "serialize_sig",
        deserialize_with = "deserialize_sig"
    )]
    pub signature: [u8; 64],
    /// Trust tier of the signer at write time
    pub trust_tier: TrustTier,
    /// Wall-clock timestamp (milliseconds since Unix epoch)
    pub written_at_ms: u64,
}

impl MobyRecord {
    /// Canonical bytes for signing — deterministic, excludes signature field
    pub fn canonical_bytes(&self) -> Vec<u8> {
        // Deterministic: address + payload_type + data (sorted keys)
        let mut canon = serde_json::Map::new();
        canon.insert("h3_cell".into(),    serde_json::json!(self.address.h3_cell));
        canon.insert("epoch".into(),      serde_json::json!(self.address.epoch));
        canon.insert("public_key".into(), serde_json::json!(hex::encode(self.address.public_key)));
        canon.insert("payload_type".into(), serde_json::json!(self.payload.payload_type));
        canon.insert("data".into(),       self.payload.data.clone());
        canon.insert("written_at_ms".into(), serde_json::json!(self.written_at_ms));
        serde_json::to_vec(&serde_json::Value::Object(canon)).unwrap_or_default()
    }

    /// Verify the Ed25519 signature
    pub fn verify_signature(&self) -> MobyResult<()> {
        let vk = VerifyingKey::from_bytes(&self.address.public_key)
            .map_err(|e| MobyError::InvalidPublicKey(e.to_string()))?;
        let sig = Signature::from_bytes(&self.signature);
        vk.verify_strict(&self.canonical_bytes(), &sig)
            .map_err(|_| MobyError::InvalidSignature)
    }

    /// Blake3 hash of canonical bytes — used in Merkle tree
    pub fn content_hash(&self) -> [u8; 32] {
        *blake3::hash(&self.canonical_bytes()).as_bytes()
    }
}

fn serialize_sig<S: serde::Serializer>(sig: &[u8; 64], s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&hex::encode(sig))
}

fn deserialize_sig<'de, D: serde::Deserializer<'de>>(d: D) -> Result<[u8; 64], D::Error> {
    let s = String::deserialize(d)?;
    let bytes = hex::decode(&s).map_err(serde::de::Error::custom)?;
    bytes.try_into().map_err(|_| serde::de::Error::custom("signature must be 64 bytes"))
}

// ── Epoch Root ────────────────────────────────────────────────────────────────

/// The sealed root of an epoch's Merkle tree.
/// Once sealed, an epoch is immutable — its root is published to the GEP chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochRoot {
    pub epoch: u64,
    /// Blake3 Merkle root over all records in this epoch
    pub root_hash: [u8; 32],
    /// Number of records in this epoch
    pub record_count: u64,
    /// Timestamp when epoch was sealed (ms)
    pub sealed_at_ms: u64,
    /// Hash of the previous epoch root (epoch chain)
    pub prev_root_hash: Option<[u8; 32]>,
}

impl EpochRoot {
    pub fn root_hex(&self) -> String {
        hex::encode(self.root_hash)
    }

    pub fn prev_hex(&self) -> Option<String> {
        self.prev_root_hash.map(|h| hex::encode(h))
    }
}

// ── Merkle Proof ──────────────────────────────────────────────────────────────

/// Proof that a record existed in a sealed epoch.
/// Verifiable offline with only the epoch root hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MerkleProof {
    pub record_hash: [u8; 32],
    pub epoch: u64,
    pub epoch_root: [u8; 32],
    /// Path of sibling hashes from leaf to root
    pub path: Vec<([u8; 32], ProofSide)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProofSide { Left, Right }

impl MerkleProof {
    /// Verify this proof against its epoch root
    pub fn verify(&self) -> bool {
        let mut current = self.record_hash;
        for (sibling, side) in &self.path {
            current = match side {
                ProofSide::Left  => merkle_combine(sibling, &current),
                ProofSide::Right => merkle_combine(&current, sibling),
            };
        }
        current == self.epoch_root
    }
}

/// Blake3 hash of two child hashes — the Merkle combine function
pub fn merkle_combine(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(left);
    hasher.update(right);
    *hasher.finalize().as_bytes()
}

// ── GEP Genesis ───────────────────────────────────────────────────────────────

/// The GEP genesis hash — the anchor of the entire epoch chain.
/// Every epoch root is derived from this constant.
pub const GEP_GENESIS_HASH: &str =
    "26acb5d998b63d54f2ed92851c5c565db9fe0930fc06b06091d05c0ce4ff8289";

/// The GEP cell count formula: c(r) = 2 + 120 × 7^r
pub fn gep_cell_count(resolution: u32) -> u64 {
    2 + 120 * 7u64.pow(resolution)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn composite_key_sort_order() {
        // Cell A, epoch 1
        let k1 = CompositeKey::new(0x861e8050fffffff0, 1, &[0u8; 32]);
        // Cell A, epoch 2
        let k2 = CompositeKey::new(0x861e8050fffffff0, 2, &[0u8; 32]);
        // Cell B, epoch 1
        let k3 = CompositeKey::new(0x861e8050fffffff1, 1, &[0u8; 32]);

        // Same cell: epoch 1 < epoch 2
        assert!(k1 < k2);
        // Cell A < Cell B regardless of epoch
        assert!(k1 < k3);
        assert!(k2 < k3);
    }

    #[test]
    fn gep_cell_count_res15() {
        // Res-15 should have 569,707,381,193,162 cells
        assert_eq!(gep_cell_count(15), 569_707_381_193_162);
    }

    #[test]
    fn spacetime_from_latlng() {
        // Rome centro → Res-7 H3 cell
        let addr = SpacetimeAddress::from_latlng(41.9028, 12.4964, 7, 9, [0u8; 32]).unwrap();
        assert_eq!(addr.epoch, 9);
        assert_eq!(addr.resolution().unwrap(), 7);
        println!("Rome Res-7 cell: {:x}", addr.h3_cell);
    }

    #[test]
    fn merkle_combine_deterministic() {
        let a = [1u8; 32];
        let b = [2u8; 32];
        let r1 = merkle_combine(&a, &b);
        let r2 = merkle_combine(&a, &b);
        assert_eq!(r1, r2);
        // Order matters
        assert_ne!(merkle_combine(&a, &b), merkle_combine(&b, &a));
    }
}
