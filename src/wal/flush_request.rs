use std::fmt;
use std::sync::mpsc::SyncSender;
use std::time::Instant;

use crate::ChunkId;
use crate::WalTypes;
use crate::chunk::file_slot::FileSlot;
use crate::wal::file_persisted::ChunkPersistedCallback;

/// A `WorkerRequest` tagged with a monotonically increasing sequence number.
///
/// The main thread assigns an incrementing `seq` to every request it sends.
/// After processing a request, the FlushWorker stores the highest completed
/// seq into a shared `AtomicU64`, allowing the main thread to wait until all
/// sent requests have been processed.
pub(crate) struct SeqRequest<W>
where W: WalTypes
{
    pub(crate) seq: u64,
    pub(crate) queued_at: Instant,
    pub(crate) req: WorkerRequest<W>,
}

impl<W> fmt::Debug for SeqRequest<W>
where W: WalTypes
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeqRequest")
            .field("seq", &self.seq)
            .field("queued_at", &self.queued_at)
            .finish_non_exhaustive()
    }
}

pub(crate) struct WriteRequest<W>
where W: WalTypes
{
    pub(crate) upto_offset: u64,
    pub(crate) data: Vec<u8>,
    pub(crate) sync: bool,
    pub(crate) callback: Option<W::Callback>,
}

impl<W> fmt::Debug for WriteRequest<W>
where W: WalTypes
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteRequest")
            .field("upto_offset", &self.upto_offset)
            .field("data_len", &self.data.len())
            .field("sync", &self.sync)
            .field("has_callback", &self.callback.is_some())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub struct FlushStat {
    pub starting_offset: u64,
    pub sync_id: u64,
    pub ino: u64,
}

impl FlushStat {
    #[allow(dead_code)]
    pub fn offset_sync_id(&self) -> (u64, u64) {
        (self.starting_offset, self.sync_id)
    }
}

/// Asks the flush worker to materialize the file of a newly opened chunk.
///
/// When a full chunk is closed, the caller constructs the successor chunk's
/// in-memory state immediately but does not create its file. The worker,
/// processing its FIFO queue in order, first finishes and syncs all writes
/// belonging to the predecessor chunk, and only then creates the successor
/// file and writes its leading record. This guarantees by construction that
/// a chunk file never exists on disk while its predecessor is not fully
/// durable, which is what recovery relies on: it classifies a chunk as
/// closed purely by the existence of a successor file, and repairs a torn
/// tail only on the last chunk.
pub(crate) struct RollRequest<W>
where W: WalTypes
{
    /// Global offset at which the new chunk starts; equals the end offset of
    /// the predecessor chunk.
    pub(crate) starting_offset: u64,

    /// Filesystem path of the new chunk file.
    pub(crate) path: String,

    /// Encoded leading checkpoint record, written at the start of the new
    /// file when it is created.
    pub(crate) leading_bytes: Vec<u8>,

    /// Slot shared with the new chunk's in-memory state; the worker
    /// publishes the created file handle into it.
    pub(crate) file_slot: FileSlot,

    /// Persisted callback for the new chunk file.
    pub(crate) on_persisted: ChunkPersistedCallback<W>,
}

impl<W> fmt::Debug for RollRequest<W>
where W: WalTypes
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RollRequest")
            .field("starting_offset", &ChunkId(self.starting_offset))
            .field("path", &self.path)
            .field("leading_bytes_len", &self.leading_bytes.len())
            .finish_non_exhaustive()
    }
}

pub(crate) enum WorkerRequest<W>
where W: WalTypes
{
    /// Materialize the file of a newly opened chunk, after making its
    /// predecessor durable.
    Roll(RollRequest<W>),

    /// Remove chunks that have been purged.
    ///
    /// This job must be done in FlushWorker to ensure it is after the
    /// corresponding purge record is flushed.
    RemoveChunks { chunk_paths: Vec<String> },

    /// Write data, and optionally sync all files.
    Write(WriteRequest<W>),

    /// For debug, return a list of offset and sync id of all files.
    #[allow(dead_code)]
    GetFlushStat { tx: SyncSender<Vec<FlushStat>> },
}

impl<W> fmt::Debug for WorkerRequest<W>
where W: WalTypes
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkerRequest::Roll(roll) => {
                f.debug_tuple("Roll").field(roll).finish()
            }
            WorkerRequest::RemoveChunks { chunk_paths } => f
                .debug_struct("RemoveChunks")
                .field("chunk_paths", chunk_paths)
                .finish(),
            WorkerRequest::Write(write) => {
                f.debug_tuple("Write").field(write).finish()
            }
            WorkerRequest::GetFlushStat { .. } => {
                f.debug_struct("GetFlushStat").finish_non_exhaustive()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;
    use std::sync::mpsc::SyncSender;
    use std::sync::mpsc::sync_channel;
    use std::time::Instant;

    use crate::WalTypes;
    use crate::chunk::file_slot::FileSlot;
    use crate::wal::file_persisted::ChunkPersistedCallback;
    use crate::wal::file_persisted::ChunkPersistedFn;
    use crate::wal::flush_request::FlushStat;
    use crate::wal::flush_request::RollRequest;
    use crate::wal::flush_request::SeqRequest;
    use crate::wal::flush_request::WorkerRequest;
    use crate::wal::flush_request::WriteRequest;

    #[derive(Debug, Default, Clone, PartialEq, Eq)]
    struct TestWal;

    impl WalTypes for TestWal {
        type Action = String;
        type Checkpoint = String;
        type Callback = SyncSender<Result<(), io::Error>>;
    }

    fn callback() -> ChunkPersistedCallback<TestWal> {
        let cb: ChunkPersistedFn<TestWal> = Arc::new(|_persisted, _state| {});
        ChunkPersistedCallback::new(cb, None)
    }

    #[test]
    fn test_flush_stat_offset_sync_id() {
        let stat = FlushStat {
            starting_offset: 12,
            sync_id: 34,
            ino: 56,
        };

        assert_eq!((12, 34), stat.offset_sync_id());
        assert_eq!(
            "FlushStat { starting_offset: 12, sync_id: 34, ino: 56 }",
            format!("{stat:?}")
        );
    }

    #[test]
    fn test_request_debug() -> Result<(), io::Error> {
        let (tx, _rx) = sync_channel(1);
        let write = WriteRequest::<TestWal> {
            upto_offset: 99,
            data: vec![1, 2, 3],
            sync: true,
            callback: Some(tx),
        };
        assert_eq!(
            "WriteRequest { upto_offset: 99, data_len: 3, sync: true, has_callback: true }",
            format!("{write:?}")
        );

        let req = WorkerRequest::Write(write);
        assert_eq!(
            "Write(WriteRequest { upto_offset: 99, data_len: 3, sync: true, has_callback: true })",
            format!("{req:?}")
        );

        let seq_req = SeqRequest {
            seq: 7,
            queued_at: Instant::now(),
            req,
        };
        let seq_debug = format!("{seq_req:?}");
        assert!(seq_debug.contains("SeqRequest"));
        assert!(seq_debug.contains("seq: 7"));
        assert!(seq_debug.contains(".."));
        assert!(matches!(seq_req.req, WorkerRequest::Write(_)));

        let remove = WorkerRequest::<TestWal>::RemoveChunks {
            chunk_paths: vec!["a".to_string(), "b".to_string()],
        };
        assert_eq!(
            "RemoveChunks { chunk_paths: [\"a\", \"b\"] }",
            format!("{remove:?}")
        );

        let (tx, _rx) = sync_channel(1);
        let stat = WorkerRequest::<TestWal>::GetFlushStat { tx };
        assert_eq!("GetFlushStat { .. }", format!("{stat:?}"));

        let roll = WorkerRequest::<TestWal>::Roll(RollRequest {
            starting_offset: 12,
            path: "some/chunk".to_string(),
            leading_bytes: vec![1, 2, 3],
            file_slot: FileSlot::pending(),
            on_persisted: callback(),
        });
        assert_eq!(
            "Roll(RollRequest { starting_offset: ChunkId(12), \
             path: \"some/chunk\", leading_bytes_len: 3, .. })",
            format!("{roll:?}")
        );

        Ok(())
    }
}
