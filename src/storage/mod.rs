//! Storage layer - KV store and MVCC implementation
//!
//! This module provides:
//! - Abstract storage engine trait
//! - In-memory storage implementation
//! - MVCC transaction support
//! - Ordered key encoding for prefix scanning

pub mod mvcc;
pub mod engine;
pub mod memory;
pub mod keycode;