use std::{collections::HashSet, sync::{Arc, Mutex, MutexGuard}, u64};

use serde::{Deserialize, Serialize};

use crate::{error::{Error, Result}, storage::engine::Engine};

/// Transaction version number type
pub type Version = u64;

/// MVCC storage engine wrapper
///
/// Uses the underlying storage engine (Engine trait) for CRUD operations.
pub struct Mvcc<E: Engine> {
    engine: Arc<Mutex<E>>,
}

impl<E: Engine> Clone for Mvcc<E> {
    fn clone(&self) -> Self {
        Self { engine: self.engine.clone() }
    }
}

impl<E: Engine> Mvcc<E> {
    pub fn new(eng: E) -> Self {
        Self {
            engine: Arc::new(Mutex::new(eng)),
        }
    }

    pub fn begin(&self) -> Result<MvccTransaction<E>> {
        MvccTransaction::begin(self.engine.clone())
    }
}

/// MVCC transaction
pub struct MvccTransaction<E: Engine> {
    engine: Arc<Mutex<E>>,
    state: TransactionState,
}

/// Transaction state for visibility checks
pub struct TransactionState {
    /// Current transaction version
    pub version: Version,
    /// Set of active transaction versions
    pub active_versions: HashSet<Version>,
}

impl TransactionState {
    /// Checks if a version is visible to this transaction
    fn is_visible(&self, version: Version) -> bool {
        if self.active_versions.contains(&version) {
            // Active transaction is modifying this key, not visible
            return false;
        } else {
            // Versions greater than this transaction are not visible
            return version <= self.version;
        }
    }
}

/// MVCC key types for storage operations
///
/// These special keys are used to store MVCC metadata in the storage engine.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum MvccKey {
    /// Next available transaction version number
    NextVersion,
    /// Active transaction version
    TxnActive(Version),
    /// Write set entry: tracks which keys were modified by a transaction (for rollback)
    TxnWrite(Version, #[serde(with = "serde_bytes")] Vec<u8>), 
    /// Versioned key: associates a key with its version for correct data retrieval
    Version(#[serde(with = "serde_bytes")] Vec<u8>, Version),
}

/*
Bincode encoding format for enum variants:
  NextVersion:    0
  TxnActive:      1-100, 1-101, 1-102
  TxnWrite:       2-version-key
  Version:        3-key1-101, 3-key2-101
*/

impl MvccKey {
    /// Serializes the key to bytes for storage
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    /// Deserializes bytes back to a key
    pub fn decode(data: Vec<u8>) -> Result<Self> {
        Ok(bincode::deserialize(&data)?)
    }
}

/// MVCC key prefix types for prefix scanning
///
/// In bincode, enums are serialized as [variant_index][variant_data...].
/// This enum aligns with MvccKey variant indices for prefix scanning.
#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKeyPrefix {
    NextVersion,
    TxnActive,
    TxnWrite(Version),
    Version(#[serde(with = "serde_bytes")] Vec<u8>),
}

impl MvccKeyPrefix {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}

impl<E: Engine> MvccTransaction<E> {
    /// Begins a new transaction
    pub fn begin(eng: Arc<Mutex<E>>) -> Result<Self> {
        let mut engine = eng.lock()?;

        // Get the next version number (stored as "next" in storage)
        let next_version = match engine.get(MvccKey::NextVersion.encode())? {
            Some(value) => bincode::deserialize(&value)?,
            None => 1, // First transaction ever
        };

        // Increment and save the next version number
        engine.set(
            MvccKey::NextVersion.encode(),
            bincode::serialize(&(next_version + 1))?
        )?;

        // Get current active transaction list
        let active_versions = Self::scan_active(&mut engine)?;

        // Add this transaction to active list (value is empty)
        engine.set(MvccKey::TxnActive(next_version).encode(), vec![])?;

        Ok(Self {
            engine: eng.clone(),
            state: TransactionState {
                version: next_version,
                active_versions,
            }
        })
    }

    /// Commits the transaction
    ///
    /// Cleans up TxnWrite entries and removes from active list.
    /// Does not delete the actual data (unlike rollback).
    pub fn commit(&self) -> Result<()> {
        let mut engine = self.engine.lock()?;

        let mut delete_keys = Vec::new();
        // Find all TxnWrite entries for this transaction
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode());
        while let Some((key, _)) = iter.next().transpose()? {
            delete_keys.push(key);
        }
        drop(iter);

        for key in delete_keys.into_iter() {
            engine.delete(key)?;
        }

        // Remove from active transaction list
        engine.delete(MvccKey::TxnActive(self.state.version).encode())
    }

    /// Rolls back the transaction
    ///
    /// Deletes TxnWrite entries, Version entries (actual data), and TxnActive entry.
    pub fn rollback(&self) -> Result<()> {
        let mut engine = self.engine.lock()?;
        let mut delete_keys = Vec::new();

        // Find all TxnWrite entries for this transaction
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode());
        while let Some((key, _)) = iter.next().transpose()? {
            // Decode to get the raw key, then add Version entry to delete list
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnWrite(_, raw_key) => {
                    delete_keys.push(MvccKey::Version(raw_key, self.state.version).encode());
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
            delete_keys.push(key);
        }
        // Drop iterator to release borrow before using engine again
        drop(iter);

        for key in delete_keys.into_iter() {
            engine.delete(key)?;
        }

        // Remove from active transaction list
        engine.delete(MvccKey::TxnActive(self.state.version).encode())
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.write_inner(key, Some(value))
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        self.write_inner(key, None)
    }

    /// Gets the value for a key, respecting MVCC visibility
    ///
    /// Scans versions from newest to oldest and returns the first visible version.
    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let mut engine = self.engine.lock()?;

        // Scan version range [0, current_version]
        let from = MvccKey::Version(key.clone(), 0).encode();
        let to = MvccKey::Version(key.clone(), self.state.version).encode();
        let mut iter = engine.scan(from..=to).rev();

        // Reverse scan to find the newest visible version
        while let Some((key, value)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::Version(_, version) => {
                    if self.state.is_visible(version) {
                        return Ok(bincode::deserialize(&value)?);
                    }
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
        }
        Ok(None)
    }

    pub fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Vec<ScanResult>> {
        let mut eng = self.engine.lock()?;
        let mut iter = eng.scan_prefix(prefix);
        let mut results = Vec::new();
        while let Some((key, value)) = iter.next().transpose()? {
            results.push(ScanResult { key, value });
        }
        Ok(results)
    }

    /// Internal write operation with conflict detection
    fn write_inner(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        let mut engine = self.engine.lock()?;

        // Conflict detection: scan for versions of this key
        // Example: active transactions are 3, 4, 5; current is 6
        // If transaction 3 modified key1, 4 modified key2, etc.
        let from = MvccKey::Version(
            key.clone(),
            self.state
                .active_versions
                .iter()
                .min()
                .copied()
                .unwrap_or(self.state.version + 1),
        )
        .encode();
        let to = MvccKey::Version(key.clone(), u64::MAX).encode();

        // Check the last version of this key for conflicts:
        // 1. Keys are scanned in order (smallest to largest)
        // 2. If a newer committed transaction (e.g., 10) modified this key,
        //    current transaction (6) has a conflict
        // 3. If an active transaction modified this key, other active transactions
        //    couldn't have modified it
        if let Some((k, _)) = engine.scan(from..=to).last().transpose()? {
            match MvccKey::decode(k.clone())? {
                MvccKey::Version(_, version) => {
                    if !self.state.is_visible(version) {
                        return Err(Error::WriteConflict);
                    }
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(k)
                    )))
                }
            }
        }

        // Record this write for potential rollback
        engine.set(
            MvccKey::TxnWrite(self.state.version, key.clone()).encode(),
            vec![]
        )?;

        // Write the actual versioned data
        engine.set(
            MvccKey::Version(key.clone(), self.state.version).encode(),
            bincode::serialize(&value)?,
        )?;

        Ok(())
    }

    /// Scans for active transaction versions using prefix scan
    ///
    /// Since TxnActive keys are encoded with prefix 1, we can find all
    /// active transactions by scanning with the TxnActive prefix.
    fn scan_active(engine: &mut MutexGuard<E>) -> Result<HashSet<Version>> {
        let mut active_versions = HashSet::new();
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnActive.encode());

        while let Some((key, _)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnActive(version) => {
                    active_versions.insert(version);
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
        }
        Ok(active_versions)
    }
}

/// Scan result containing key-value pair
pub struct ScanResult {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
