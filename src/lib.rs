//! Chunked write-ahead log.

pub mod api;
pub mod errors;
pub mod stat;
pub mod types;
pub mod wal;

mod chunk;
mod config;
mod num;
mod offset_reader;

pub use api::state_machine::StateMachine;
pub use api::wal::WAL;
pub use api::wal_types::WalTypes;
pub use chunk::Chunk;
pub use chunk::chunk_id::ChunkId;
pub use chunk::closed_chunk::ClosedChunk;
pub use chunk::open_chunk::OpenChunk;
pub use config::Config;
pub use stat::ChunkStat;
pub use stat::FlushLatencyPercentiles;
pub use stat::FlushMetrics;
pub use wal::ChunkedWal;
pub use wal::FlushStat;
pub use wal::callback::Callback;
pub use wal::file_persisted::ChunkPersisted;
pub use wal::file_persisted::ChunkPersistedFn;
pub use wal::wal_record::WALRecord;

pub use crate::types::Segment;
