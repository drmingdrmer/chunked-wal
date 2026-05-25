use std::fmt;
use std::sync::mpsc::SyncSender;
use std::time::Instant;

use crate::WalTypes;
use crate::wal::file_entry::FileEntry;

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

pub(crate) enum WorkerRequest<W>
where W: WalTypes
{
    /// Append a new file that will be need to be sync.
    AppendFile(FileEntry<W>),

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
            WorkerRequest::AppendFile(file_entry) => {
                f.debug_tuple("AppendFile").field(file_entry).finish()
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
    use crate::wal::file_entry::FileEntry;
    use crate::wal::file_persisted::ChunkPersistedCallback;
    use crate::wal::file_persisted::ChunkPersistedFn;
    use crate::wal::flush_request::FlushStat;
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

        let file = Arc::new(tempfile::tempfile()?);
        let append = WorkerRequest::AppendFile(FileEntry::<TestWal>::new(
            12,
            file,
            callback(),
        ));
        assert_eq!(
            "AppendFile(FileEntry { starting_offset: ChunkId(12), sync_id: 0 })",
            format!("{append:?}")
        );

        Ok(())
    }
}
