use std::sync::{Arc, Mutex};

use crate::{error::Result, storage::engine::Engine};

// Mvcc通过调用底层的存储引擎Engine这个Trait来CRUD，所以可以给它加个范型参数
pub struct Mvcc<E: Engine> {
    // Arc = 原子引用计数，允许多个事务共享同一个引擎
    // Mutex = 互斥锁，保证同时只有一个线程访问引擎
    engine: Arc<Mutex<E>>, // 多线程安全
}

impl<E: Engine> Clone for Mvcc<E> {
    fn clone(&self) -> Self {
        Self { engine: self.engine.clone(), }
    }
}

impl<E: Engine> Mvcc<E> {
    pub fn new(eng: E) -> Self {
        Self {
            engine: Arc::new(Mutex::new(eng)),
        }
    }

    pub fn begin(&self) -> Result<MvccTransaction<E>> {
        Ok(MvccTransaction::begin(self.engine.clone())) // 每个事务用同一个引擎(但是互斥使用)
    }
}

pub struct MvccTransaction<E: Engine> {
    engine: Arc<Mutex<E>>
}

impl<E: Engine> MvccTransaction<E> {
    pub fn begin(eng: Arc<Mutex<E>>) -> Self {
        Self {
            engine: eng,
        }
    }

    pub fn commit(&self) -> Result<()> {
        Ok(())
    }

    pub fn rollback(&self) -> Result<()> {
        Ok(())
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut eng = self.engine.lock()?;
        eng.set(key, value) // 调用底层存储
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
            results.push(ScanResult{ key, value });
        }
        Ok(results)
    }
}

pub struct ScanResult {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
