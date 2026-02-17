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
#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKey {
    /// Next available transaction version number
    NextVersion,
    /// Active transaction version
    TxnAcvtive(Version),
    /// Write set entry: tracks which keys were modified by a transaction (for rollback)
    TxnWrite(Version, Vec<u8>),
    /// Versioned key: associates a key with its version for correct data retrieval
    Version(Vec<u8>, Version),
}

/*
Bincode encoding format for enum variants:
  NextVersion:    0
  TxnAcvtive:     1-100, 1-101, 1-102
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
    TxnAcvtive,
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
        engine.set(MvccKey::TxnAcvtive(next_version).encode(), vec![])?;

        Ok(Self {
            engine: eng.clone(),
            state: TransactionState {
                version: next_version,
                active_versions,
            }
        })
    }

    pub fn commit(&self) -> Result<()> {
        Ok(())
    }

    pub fn rollback(&self) -> Result<()> {
        Ok(())
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.write_inner(key, Some(value))
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        self.write_inner(key, None)
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let mut eng = self.engine.lock()?;
        eng.get(key)
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
    /// Since TxnAcvtive keys are encoded with prefix 1, we can find all
    /// active transactions by scanning with the TxnAcvtive prefix.
    fn scan_active(engine: &mut MutexGuard<E>) -> Result<HashSet<Version>> {
        let mut active_versions = HashSet::new();
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnAcvtive.encode());

        while let Some((key, _)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnAcvtive(version) => {
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
