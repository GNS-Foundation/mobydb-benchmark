/// MobyDB Merkle — Epoch Sealing & Proof Generation
///
/// At the close of each epoch, all records are hashed into a Merkle tree.
/// The root is sealed and published to the GEP chain.
/// Any record can then produce an offline-verifiable proof of existence.

use mobydb_core::{merkle_combine, EpochRoot, MerkleProof, MobyError,
                   MobyResult, ProofSide, GEP_GENESIS_HASH};
use mobydb_storage::MobyStore;
use tracing::info;

// ── EpochEngine ───────────────────────────────────────────────────────────────

pub struct EpochEngine<'a> {
    store: &'a MobyStore,
}

impl<'a> EpochEngine<'a> {
    pub fn new(store: &'a MobyStore) -> Self {
        Self { store }
    }

    /// Seal an epoch: collect all records, build Merkle tree, store root.
    /// Called at the end of each epoch window (e.g. every hour).
    ///
    /// The epoch root is derived from:
    ///   - All record content hashes in this epoch
    ///   - The previous epoch's root hash (chain continuity)
    pub fn seal_epoch(&self, epoch: u64) -> MobyResult<EpochRoot> {
        if self.store.is_epoch_sealed(epoch)? {
            return Err(MobyError::EpochSealed(epoch));
        }

        // 1. Collect all records in this epoch
        let records = self.store.scan_epoch(epoch)?;
        let record_count = records.len() as u64;
        info!("Sealing epoch {}: {} records", epoch, record_count);

        // 2. Compute leaf hashes (Blake3 of each record's canonical bytes)
        let mut leaves: Vec<[u8; 32]> = records.iter()
            .map(|r| r.content_hash())
            .collect();

        // 3. Get previous epoch root for chain continuity
        let prev_root_hash = if epoch > 0 {
            self.store.read_epoch_root(epoch - 1)
                .ok()
                .map(|r| r.root_hash)
        } else {
            // Epoch 0: anchor to GEP genesis hash
            let genesis = hex::decode(GEP_GENESIS_HASH)
                .map_err(|e| MobyError::Storage(e.to_string()))?;
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&genesis[..32]);
            Some(arr)
        };

        // 4. Include previous root as an extra leaf (chain continuity)
        if let Some(prev) = prev_root_hash {
            leaves.push(prev);
        }

        // 5. Build Merkle tree
        let root_hash = Self::build_merkle_root(&leaves);

        // 6. Seal
        let sealed_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let root = EpochRoot {
            epoch,
            root_hash,
            record_count,
            sealed_at_ms,
            prev_root_hash,
        };

        self.store.store_epoch_root(&root)?;
        info!("Epoch {} sealed: root={}", epoch, root.root_hex());

        Ok(root)
    }

    /// Generate a Merkle proof for a specific record.
    /// Proves the record was included in the given epoch.
    pub fn generate_proof(
        &self,
        address: &mobydb_core::SpacetimeAddress,
    ) -> MobyResult<MerkleProof> {
        let epoch = address.epoch;

        // Epoch must be sealed
        if !self.store.is_epoch_sealed(epoch)? {
            return Err(MobyError::Storage(
                format!("epoch {} is not yet sealed — proof unavailable", epoch)
            ));
        }

        let epoch_root = self.store.read_epoch_root(epoch)?;
        let records = self.store.scan_epoch(epoch)?;

        // Find target record
        let target_record = self.store.read(address)?;
        let target_hash = target_record.content_hash();

        // Build leaves (same order as seal_epoch)
        let mut leaves: Vec<[u8; 32]> = records.iter()
            .map(|r| r.content_hash())
            .collect();

        if let Some(prev) = epoch_root.prev_root_hash {
            leaves.push(prev);
        }

        // Find target index
        let target_idx = leaves.iter()
            .position(|&h| h == target_hash)
            .ok_or(MobyError::NotFound)?;

        // Generate Merkle path
        let path = Self::merkle_path(&leaves, target_idx);

        Ok(MerkleProof {
            record_hash: target_hash,
            epoch,
            epoch_root: epoch_root.root_hash,
            path,
        })
    }

    // ── Merkle Tree ───────────────────────────────────────────────────────────

    /// Build a Merkle root from a list of leaf hashes.
    /// Uses Blake3 as the hash function throughout.
    pub fn build_merkle_root(leaves: &[[u8; 32]]) -> [u8; 32] {
        if leaves.is_empty() {
            // Empty epoch: hash of empty bytes
            return *blake3::hash(&[]).as_bytes();
        }
        if leaves.len() == 1 {
            return leaves[0];
        }

        let mut current = leaves.to_vec();

        while current.len() > 1 {
            let mut next = Vec::with_capacity((current.len() + 1) / 2);
            let mut i = 0;
            while i < current.len() {
                if i + 1 < current.len() {
                    next.push(merkle_combine(&current[i], &current[i + 1]));
                } else {
                    // Odd leaf: duplicate (standard practice)
                    next.push(merkle_combine(&current[i], &current[i]));
                }
                i += 2;
            }
            current = next;
        }

        current[0]
    }

    /// Generate the Merkle inclusion path for a leaf at `index`.
    fn merkle_path(leaves: &[[u8; 32]], index: usize) -> Vec<([u8; 32], ProofSide)> {
        let mut path = Vec::new();
        let mut current = leaves.to_vec();
        let mut idx = index;

        while current.len() > 1 {
            let mut next = Vec::with_capacity((current.len() + 1) / 2);
            let mut i = 0;

            while i < current.len() {
                let left  = current[i];
                let right = if i + 1 < current.len() { current[i + 1] } else { current[i] };

                next.push(merkle_combine(&left, &right));

                // Record sibling for our path
                if i == idx || i + 1 == idx {
                    if i == idx {
                        // Our node is left — sibling is right
                        let sibling = if i + 1 < current.len() { right } else { left };
                        path.push((sibling, ProofSide::Right));
                    } else {
                        // Our node is right (i+1 == idx) — sibling is left
                        path.push((left, ProofSide::Left));
                    }
                }
                i += 2;
            }

            idx /= 2;
            current = next;
        }

        path
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use mobydb_core::{merkle_combine, MerkleProof};

    #[test]
    fn merkle_root_empty() {
        let root = EpochEngine::build_merkle_root(&[]);
        // Deterministic
        assert_eq!(root, EpochEngine::build_merkle_root(&[]));
    }

    #[test]
    fn merkle_root_single() {
        let leaf = [1u8; 32];
        let root = EpochEngine::build_merkle_root(&[leaf]);
        assert_eq!(root, leaf);
    }

    #[test]
    fn merkle_root_two_leaves() {
        let a = [1u8; 32];
        let b = [2u8; 32];
        let root = EpochEngine::build_merkle_root(&[a, b]);
        assert_eq!(root, merkle_combine(&a, &b));
    }

    #[test]
    fn merkle_proof_verifies() {
        let leaves: Vec<[u8; 32]> = (0..8u8).map(|i| [i; 32]).collect();
        let root = EpochEngine::build_merkle_root(&leaves);

        // Generate proof for leaf 3
        let path = EpochEngine::merkle_path(&leaves, 3);
        let proof = MerkleProof {
            record_hash: leaves[3],
            epoch: 9,
            epoch_root: root,
            path,
        };

        assert!(proof.verify(), "Merkle proof failed to verify");
    }

    #[test]
    fn merkle_proof_tampered_fails() {
        let leaves: Vec<[u8; 32]> = (0..4u8).map(|i| [i; 32]).collect();
        let root = EpochEngine::build_merkle_root(&leaves);

        let path = EpochEngine::merkle_path(&leaves, 1);
        let mut proof = MerkleProof {
            record_hash: leaves[1],
            epoch: 9,
            epoch_root: root,
            path,
        };

        // Tamper with the record hash
        proof.record_hash[0] ^= 0xFF;
        assert!(!proof.verify(), "Tampered proof should not verify");
    }

    #[test]
    fn merkle_proof_all_leaves_verify() {
        let leaves: Vec<[u8; 32]> = (0..7u8).map(|i| [i + 1; 32]).collect();
        let root = EpochEngine::build_merkle_root(&leaves);

        for (i, &leaf) in leaves.iter().enumerate() {
            let path = EpochEngine::merkle_path(&leaves, i);
            let proof = MerkleProof {
                record_hash: leaf,
                epoch: 9,
                epoch_root: root,
                path,
            };
            assert!(proof.verify(), "Proof for leaf {} failed", i);
        }
    }
}
