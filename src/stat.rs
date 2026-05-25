use std::fmt;
use std::fmt::Formatter;

use crate::ChunkId;
use crate::num::format_pad9_u64;

/// Aggregated flush worker metrics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlushMetrics {
    /// Number of write batches processed by the flush worker.
    pub batch_count: u64,
    /// Number of batches that required a filesystem sync.
    pub sync_batch_count: u64,
    /// Number of write requests included in all batches.
    pub write_request_count: u64,
    /// Total bytes written by the flush worker.
    pub write_bytes: u64,
    /// Number of callbacks sent after write batches.
    pub callback_count: u64,
    /// Number of times the flush worker intentionally waited for batching.
    pub group_wait_count: u64,
    /// Total intentional group-commit wait time, in microseconds.
    pub group_wait_us: u64,
    /// Maximum intentional group-commit wait time, in microseconds.
    pub group_wait_max_us: u64,
    /// Total request queue wait before a batch starts writing, in
    /// microseconds.
    pub queued_wait_us: u64,
    /// Maximum request queue wait before a batch starts writing, in
    /// microseconds.
    pub queued_wait_max_us: u64,
    /// Total file write time, in microseconds.
    pub write_us: u64,
    /// Maximum file write time for one batch, in microseconds.
    pub write_max_us: u64,
    /// Total filesystem sync time, in microseconds.
    pub sync_us: u64,
    /// Maximum filesystem sync time for one batch, in microseconds.
    pub sync_max_us: u64,
    /// Total batch processing time, in microseconds.
    pub batch_us: u64,
    /// Maximum batch processing time, in microseconds.
    pub batch_max_us: u64,
    /// Largest number of write requests in one batch.
    pub batch_size_max: u64,
    /// Largest number of bytes written by one batch.
    pub batch_bytes_max: u64,
    /// Number of write requests in the latest batch.
    pub last_batch_size: u64,
    /// Number of bytes written by the latest batch.
    pub last_batch_bytes: u64,
    /// Number of callbacks sent by the latest batch.
    pub last_callback_count: u64,
    /// Filesystem sync duration of the latest batch, in microseconds.
    pub last_sync_us: u64,
    /// Request queue wait maximum of the latest batch, in microseconds.
    pub last_queued_wait_max_us: u64,
    /// Intentional group-commit wait latency percentiles, in microseconds.
    pub group_wait_percentiles: FlushLatencyPercentiles,
    /// Per-batch max request queue wait latency percentiles, in microseconds.
    pub queued_wait_percentiles: FlushLatencyPercentiles,
    /// File write latency percentiles, in microseconds.
    pub write_percentiles: FlushLatencyPercentiles,
    /// Filesystem sync latency percentiles, in microseconds.
    pub sync_percentiles: FlushLatencyPercentiles,
    /// Whole batch processing latency percentiles, in microseconds.
    pub batch_percentiles: FlushLatencyPercentiles,
}

/// Percentiles for one flush worker latency dimension.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlushLatencyPercentiles {
    pub p50_us: u64,
    pub p90_us: u64,
    pub p99_us: u64,
}

/// Statistics about a single chunk in the WAL.
#[derive(Debug, Clone)]
pub struct ChunkStat<Chkp> {
    /// Unique identifier for this chunk.
    pub chunk_id: ChunkId,
    /// Number of records stored in this chunk.
    pub records_count: u64,
    /// Global offset of the first record in this chunk.
    pub global_start: u64,
    /// Global offset after the last record in this chunk.
    pub global_end: u64,
    /// Size of the chunk in bytes.
    pub size: u64,
    /// Checkpoint stored for this chunk.
    pub log_state: Chkp,
}

impl<Chkp> fmt::Display for ChunkStat<Chkp>
where Chkp: fmt::Debug
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ChunkStat({}){{records: {}, [{}, {}), size: {}, log_state: {:?}}}",
            self.chunk_id,
            self.records_count,
            format_pad9_u64(self.global_start),
            format_pad9_u64(self.global_end),
            format_pad9_u64(self.size),
            self.log_state
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::ChunkId;
    use crate::stat::ChunkStat;
    use crate::stat::FlushLatencyPercentiles;
    use crate::stat::FlushMetrics;

    #[test]
    fn test_chunk_stat_display() {
        let stat = ChunkStat {
            chunk_id: ChunkId(12),
            records_count: 3,
            global_start: 12,
            global_end: 45,
            size: 33,
            log_state: "checkpoint",
        };

        assert_eq!(
            "ChunkStat(ChunkId(00_000_000_000_000_000_012)){records: 3, [000_000_012, 000_000_045), size: 000_000_033, log_state: \"checkpoint\"}",
            stat.to_string()
        );
    }

    #[test]
    fn test_flush_metrics_default_clone_eq() {
        let metrics = FlushMetrics {
            batch_count: 1,
            sync_batch_count: 2,
            write_request_count: 3,
            write_bytes: 4,
            callback_count: 5,
            group_wait_count: 6,
            group_wait_us: 7,
            group_wait_max_us: 8,
            queued_wait_us: 9,
            queued_wait_max_us: 10,
            write_us: 11,
            write_max_us: 12,
            sync_us: 13,
            sync_max_us: 14,
            batch_us: 15,
            batch_max_us: 16,
            batch_size_max: 17,
            batch_bytes_max: 18,
            last_batch_size: 19,
            last_batch_bytes: 20,
            last_callback_count: 21,
            last_sync_us: 22,
            last_queued_wait_max_us: 23,
            group_wait_percentiles: FlushLatencyPercentiles {
                p50_us: 24,
                p90_us: 25,
                p99_us: 26,
            },
            queued_wait_percentiles: FlushLatencyPercentiles::default(),
            write_percentiles: FlushLatencyPercentiles::default(),
            sync_percentiles: FlushLatencyPercentiles::default(),
            batch_percentiles: FlushLatencyPercentiles::default(),
        };

        assert_eq!(metrics, metrics.clone());
        assert_eq!(
            FlushLatencyPercentiles {
                p50_us: 0,
                p90_us: 0,
                p99_us: 0,
            },
            FlushLatencyPercentiles::default()
        );
    }
}
