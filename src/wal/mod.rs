pub mod callback;
pub mod file_persisted;
pub mod wal_record;

pub(crate) mod atomic_flush_metrics;
pub(crate) mod batch_metrics;
mod closed_chunk_reader;
pub(crate) mod file_entry;
pub(crate) mod flush_request;
pub(crate) mod flush_worker;
pub(crate) mod queued_write;
pub(crate) mod write_batch;

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc::SyncSender;
use std::time::Instant;

pub use closed_chunk_reader::ClosedChunkReader;
use codeq::OffsetSize;
pub use flush_request::FlushStat;
pub(crate) use flush_request::WorkerRequest;
use log::info;

use crate::Chunk;
use crate::ChunkId;
use crate::Config;
use crate::WALRecord;
use crate::WalLock;
use crate::WalTypes;
use crate::api::state_machine::StateMachine;
use crate::api::wal::WAL;
use crate::chunk::closed_chunk::ClosedChunk;
use crate::chunk::open_chunk::OpenChunk;
use crate::num::format_pad_u64;
use crate::stat::ChunkStat;
use crate::stat::FlushMetrics;
use crate::types::Segment;
use crate::wal::atomic_flush_metrics::AtomicFlushMetrics;
use crate::wal::file_entry::FileEntry;
use crate::wal::file_persisted::ChunkPersisted;
use crate::wal::file_persisted::ChunkPersistedCallback;
pub use crate::wal::file_persisted::ChunkPersistedFn;
use crate::wal::flush_request::SeqRequest;
use crate::wal::flush_request::WriteRequest;
use crate::wal::flush_worker::FlushWorker;

/// Chunked write-ahead log implementation.
///
/// This WAL implementation manages both open and closed chunks of data.
/// An open chunk is actively being written to, while closed chunks are
/// immutable and may be used for reading historical data.
pub struct ChunkedWal<W>
where W: WalTypes
{
    config: Arc<Config>,
    open: OpenChunk<WALRecord<W>>,
    closed: BTreeMap<ChunkId, ClosedChunk<W>>,

    /// Sends user write operations to the flush worker.
    ///
    /// Each write operation may carry its own callback, defined by
    /// `W::Callback`.
    flush_tx: SyncSender<SeqRequest<W>>,

    /// File-level callback invoked after fsync.
    ///
    /// This callback is called once for each synced chunk file.
    on_chunk_persisted: ChunkPersistedFn<W>,

    /// The next sequence number to assign. Incremented on each `send_request`.
    /// Only accessed by the main thread, so a plain `u64` suffices.
    sent_seq: u64,

    /// Shared with `FlushWorker`; stores the highest completed seq.
    done_seq: Arc<AtomicU64>,

    /// Shared with `FlushWorker`; stores aggregated flush metrics.
    flush_metrics: Arc<AtomicFlushMetrics>,

    /// Holds the exclusive lock on the WAL directory for this WAL instance.
    _dir_lock: WalLock,
}

impl<W> fmt::Debug for ChunkedWal<W>
where W: WalTypes
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChunkedWal")
            .field("config", &self.config)
            .field("open", &self.open)
            .field("closed", &self.closed)
            .field("sent_seq", &self.sent_seq)
            .field("done_seq", &self.done_seq)
            .field("flush_metrics", &self.flush_metrics)
            .finish_non_exhaustive()
    }
}

impl<W> ChunkedWal<W>
where W: WalTypes
{
    /// Opens a ChunkedWal instance and replays existing records into a state
    /// machine.
    pub fn open<SM>(
        config: Arc<Config>,
        state_machine: &mut SM,
        on_chunk_persisted: ChunkPersistedFn<W>,
    ) -> Result<Self, io::Error>
    where
        SM: StateMachine<W>,
    {
        let dir_lock = Self::acquire_lock(&config)?;
        Self::open_locked(config, state_machine, on_chunk_persisted, dir_lock)
    }

    /// Acquires the exclusive WAL directory lock.
    pub fn acquire_lock(config: &Config) -> Result<WalLock, io::Error> {
        WalLock::new(config)
    }

    /// Opens a ChunkedWal instance with an already-held WAL directory lock.
    pub fn open_locked<SM>(
        config: Arc<Config>,
        state_machine: &mut SM,
        on_chunk_persisted: ChunkPersistedFn<W>,
        dir_lock: WalLock,
    ) -> Result<Self, io::Error>
    where
        SM: StateMachine<W>,
    {
        let chunk_ids = Self::load_chunk_ids(&config, &dir_lock)?;

        let mut closed = BTreeMap::new();
        let mut prev_end_offset = None;
        let mut prev_checkpoint = None;

        for chunk_id in chunk_ids.iter().copied() {
            Self::ensure_consecutive_chunks(prev_end_offset, chunk_id)?;

            let (chunk, records) =
                Chunk::<WALRecord<W>>::open(config.clone(), chunk_id)?;

            on_chunk_persisted(
                ChunkPersisted {
                    file: chunk.f.clone(),
                    starting_offset: chunk.global_start(),
                    synced_offset: chunk.global_end(),
                },
                prev_checkpoint.clone(),
            );

            for (i, record) in records.iter().enumerate() {
                let seg = chunk.record_segment(i);
                state_machine
                    .apply(record, chunk_id, seg)
                    .map_err(|e| io::Error::other(e.to_string()))?;
            }

            prev_end_offset = Some(chunk.last_segment().end().0);
            let checkpoint = Arc::new(state_machine.checkpoint());
            prev_checkpoint = Some(checkpoint.clone());

            closed.insert(chunk_id, ClosedChunk::new(chunk, checkpoint));
        }

        let open = Self::reopen_last_closed(&mut closed);

        let open = if let Some(open) = open {
            open
        } else {
            OpenChunk::create(
                config.clone(),
                ChunkId(prev_end_offset.unwrap_or_default()),
                WALRecord::Checkpoint(state_machine.checkpoint()),
            )?
        };

        Ok(Self::new(
            config,
            closed,
            open,
            on_chunk_persisted,
            dir_lock,
        ))
    }

    /// Dumps all records while holding the WAL directory lock.
    pub fn dump_records<D>(
        config: &Config,
        _dir_lock: &WalLock,
        mut write_record: D,
    ) -> Result<(), io::Error>
    where
        D: FnMut(
            ChunkId,
            u64,
            Result<(Segment, WALRecord<W>), io::Error>,
        ) -> Result<(), io::Error>,
    {
        let chunk_ids = Self::load_chunk_ids(config, _dir_lock)?;
        for chunk_id in chunk_ids {
            let it = Chunk::<WALRecord<W>>::dump(config, chunk_id)?;
            for (i, res) in it.into_iter().enumerate() {
                write_record(chunk_id, i as u64, res)?;
            }
        }

        Ok(())
    }

    /// Creates a new ChunkedWal instance after recovery has completed.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for the WAL
    /// * `closed` - Map of closed (immutable) chunks indexed by chunk ID
    /// * `open` - The currently active chunk that can be written to
    /// * `on_chunk_persisted` - Callback invoked after chunk data is persisted
    fn new(
        config: Arc<Config>,
        closed: BTreeMap<ChunkId, ClosedChunk<W>>,
        open: OpenChunk<WALRecord<W>>,
        on_chunk_persisted: ChunkPersistedFn<W>,
        dir_lock: WalLock,
    ) -> Self {
        let prev_checkpoint =
            closed.iter().last().map(|(_, c)| c.state.clone());

        let offset = open.chunk.global_start();
        let f = open.chunk.f.clone();

        let file_entry = FileEntry::new(
            offset,
            f,
            ChunkPersistedCallback::new(
                on_chunk_persisted.clone(),
                prev_checkpoint,
            ),
        );

        let done_seq = Arc::new(AtomicU64::new(0));
        let flush_metrics = Arc::new(AtomicFlushMetrics::default());

        let (flush_tx, rx) = std::sync::mpsc::sync_channel(1024);
        let worker = FlushWorker::new(
            rx,
            file_entry,
            done_seq.clone(),
            flush_metrics.clone(),
            config.flush_batch_wait(),
            config.flush_batch_max_items(),
        );

        worker.spawn();

        Self {
            config,
            open,
            closed,
            flush_tx,
            on_chunk_persisted,
            sent_seq: 0,
            done_seq,
            flush_metrics,
            _dir_lock: dir_lock,
        }
    }

    fn ensure_consecutive_chunks(
        prev_end_offset: Option<u64>,
        chunk_id: ChunkId,
    ) -> Result<(), io::Error> {
        let Some(prev_end) = prev_end_offset else {
            return Ok(());
        };

        if prev_end != chunk_id.offset() {
            let message = format!(
                "Gap between chunks: {} -> {}; Can not open, \
                        fix this error and re-open",
                format_pad_u64(prev_end),
                format_pad_u64(chunk_id.offset()),
            );
            return Err(io::Error::new(io::ErrorKind::InvalidData, message));
        }

        Ok(())
    }

    fn reopen_last_closed(
        closed_chunks: &mut BTreeMap<ChunkId, ClosedChunk<W>>,
    ) -> Option<OpenChunk<WALRecord<W>>> {
        {
            let (_chunk_id, closed) = closed_chunks.iter().last()?;

            if closed.chunk.is_truncated() {
                return None;
            }
        }

        let (_chunk_id, last) = closed_chunks.pop_last().unwrap();
        let open = OpenChunk::new(last.chunk);
        Some(open)
    }

    pub fn load_chunk_ids(
        config: &Config,
        _dir_lock: &WalLock,
    ) -> Result<Vec<ChunkId>, io::Error> {
        let path = &config.dir;
        let entries = std::fs::read_dir(path)?;
        let mut chunk_ids = vec![];
        for entry in entries {
            let entry = entry?;
            let file_name = entry.file_name();

            let fn_str = file_name.to_string_lossy();
            if fn_str == WalLock::LOCK_FILE_NAME {
                continue;
            }

            let res = Config::parse_chunk_file_name(&fn_str);

            match res {
                Ok(offset) => {
                    chunk_ids.push(ChunkId(offset));
                }
                Err(err) => {
                    log::warn!(
                        "Ignore invalid WAL file name: '{}': {}",
                        fn_str,
                        err
                    );
                    continue;
                }
            };
        }

        chunk_ids.sort();

        Ok(chunk_ids)
    }

    pub fn open_chunk_id(&self) -> ChunkId {
        self.open.chunk.chunk_id()
    }

    pub fn closed_chunk_stats(&self) -> Vec<ChunkStat<W::Checkpoint>> {
        self.closed.values().map(|c| c.stat()).collect()
    }

    pub fn open_chunk_stat(
        &self,
        checkpoint: W::Checkpoint,
    ) -> ChunkStat<W::Checkpoint> {
        ChunkStat {
            chunk_id: self.open.chunk.chunk_id(),
            records_count: self.open.chunk.records_count() as u64,
            global_start: self.open.chunk.global_start(),
            global_end: self.open.chunk.global_end(),
            size: self.open.chunk.chunk_size(),
            log_state: checkpoint,
        }
    }

    pub fn closed_chunk_reader(&self) -> ClosedChunkReader<W> {
        ClosedChunkReader::new(self.closed.clone())
    }

    pub fn drain_closed_chunks_while<F>(
        &mut self,
        mut should_drain: F,
    ) -> Vec<ChunkId>
    where
        F: FnMut(&W::Checkpoint) -> bool,
    {
        let mut chunk_ids = Vec::new();

        while let Some((_chunk_id, closed)) = self.closed.first_key_value() {
            if !should_drain(closed.state.as_ref()) {
                break;
            }

            let (chunk_id, _closed) = self.closed.pop_first().unwrap();
            chunk_ids.push(chunk_id);
        }

        chunk_ids
    }

    pub fn dump_loaded_records<D>(
        &self,
        mut write_record: D,
    ) -> Result<(), io::Error>
    where
        D: FnMut(
            ChunkId,
            u64,
            Result<(Segment, WALRecord<W>), io::Error>,
        ) -> Result<(), io::Error>,
    {
        let closed = self.closed.keys().copied();
        let chunk_ids = closed.chain([self.open.chunk.chunk_id()]);

        for chunk_id in chunk_ids {
            let f =
                Chunk::<WALRecord<W>>::open_chunk_file(&self.config, chunk_id)?;

            let it = Chunk::<WALRecord<W>>::load_records_iter(
                &self.config,
                Arc::new(f),
                chunk_id,
            )?;

            for (i, res) in it.enumerate() {
                write_record(chunk_id, i as u64, res)?;
            }
        }

        Ok(())
    }

    pub fn on_disk_size(&self) -> u64 {
        let end = self.open.chunk.global_end();
        let open_start = self.open.chunk.global_start();
        let first_closed_start = self
            .closed
            .first_key_value()
            .map(|(_, v)| v.chunk.global_start())
            .unwrap_or(open_start);

        end - first_closed_start
    }

    pub fn last_closed_chunk_truncated_file_size(&self) -> Option<u64> {
        self.closed
            .last_key_value()
            .and_then(|(_chunk_id, closed)| closed.chunk.truncated_file_size())
    }

    /// Wraps a `WorkerRequest` with an auto-incrementing seq and sends it to
    /// the FlushWorker.
    fn send_request(&mut self, req: WorkerRequest<W>) -> Result<(), io::Error> {
        self.sent_seq += 1;
        self.flush_tx
            .send(SeqRequest {
                seq: self.sent_seq,
                queued_at: Instant::now(),
                req,
            })
            .map_err(|e| {
                io::Error::other(format!("Failed to send request: {}", e))
            })
    }

    /// Block until the FlushWorker has processed all requests sent so far.
    ///
    /// Polls `done_seq` in a 1 ms sleep loop until it reaches `sent_seq`.
    pub fn wait_worker_idle(&self) {
        while self.done_seq.load(Ordering::Relaxed) < self.sent_seq {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    pub fn flush_metrics(&self) -> FlushMetrics {
        self.flush_metrics.snapshot()
    }

    /// Hand the pending data buffer to the worker for writing.
    ///
    /// Drains `OpenChunk::pending_data` and packages it as a `WriteRequest`.
    /// The worker always writes the bytes to the OS file. When `sync` is
    /// `true` it also calls `fsync` so the data is on stable storage; when
    /// `sync` is `false` it skips the fsync and durability is deferred to
    /// the next sync write that lands in the same or a later batch.
    pub fn send_pending(
        &mut self,
        sync: bool,
        callback: Option<W::Callback>,
    ) -> Result<(), io::Error> {
        let data = self.open.take_pending_data();
        self.send_request(WorkerRequest::Write(WriteRequest {
            upto_offset: self.open.chunk.global_end(),
            data,
            sync,
            callback,
        }))
    }

    /// Requests removal of specified chunks.
    ///
    /// # Arguments
    ///
    /// * `chunk_ids` - IDs of chunk files to be removed
    ///
    /// # Errors
    ///
    /// Returns an IO error if the remove request cannot be sent
    pub fn send_remove_chunks(
        &mut self,
        chunk_ids: Vec<ChunkId>,
    ) -> Result<(), io::Error> {
        let chunk_paths = chunk_ids
            .into_iter()
            .map(|chunk_id| self.config.chunk_path(chunk_id))
            .collect();

        self.send_request(WorkerRequest::RemoveChunks { chunk_paths })
    }

    #[allow(dead_code)]
    pub fn get_stat(&mut self) -> Result<Vec<FlushStat>, io::Error> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.send_get_stat(tx)?;
        rx.recv().map_err(|e| {
            io::Error::other(format!(
                "Failed to receive get state response: {}",
                e
            ))
        })
    }

    #[allow(dead_code)]
    pub(crate) fn send_get_stat(
        &mut self,
        callback: SyncSender<Vec<FlushStat>>,
    ) -> Result<(), io::Error> {
        self.send_request(WorkerRequest::GetFlushStat { tx: callback })
    }

    /// Checks if the current open chunk has reached its capacity.
    ///
    /// Returns true if either the maximum number of records or maximum chunk
    /// size is reached.
    pub fn is_open_chunk_full(&self) -> bool {
        self.open.chunk.records_count() >= self.config.chunk_max_records()
            || (self.open.chunk.chunk_size() as usize)
                >= self.config.chunk_max_size()
    }

    /// Attempts to close the current chunk if it's full and creates a new open
    /// chunk.
    ///
    /// # Arguments
    ///
    /// * `state_machine` - The state machine that provides the checkpoint to
    ///   store at the start of the next chunk.
    ///
    /// # Returns
    ///
    /// Returns the checkpoint if a chunk was closed, None otherwise.
    ///
    /// # Errors
    ///
    /// Returns an IO error if chunk operations fail
    pub fn try_close_full_chunk<SM>(
        &mut self,
        state_machine: &SM,
    ) -> Result<Option<W::Checkpoint>, io::Error>
    where
        SM: StateMachine<W>,
    {
        if !self.is_open_chunk_full() {
            return Ok(None);
        }

        let config = self.config.clone();
        let offset = self.open.chunk.last_segment().end();

        info!(
            "Closing full chunk: {}, open new: {}",
            self.open.chunk.chunk_id(),
            ChunkId(offset.0)
        );

        let checkpoint = state_machine.checkpoint();

        let new_open = {
            let chunk_id = ChunkId(offset.0);
            OpenChunk::create(
                config,
                chunk_id,
                WALRecord::Checkpoint(checkpoint.clone()),
            )?
        };

        let mut old_open = std::mem::replace(&mut self.open, new_open);

        let prev_pending_data = old_open.take_pending_data();
        if !prev_pending_data.is_empty() {
            self.send_request(WorkerRequest::Write(WriteRequest {
                upto_offset: offset.0,
                data: prev_pending_data,
                sync: true,
                callback: None,
            }))?;
        }

        let checkpoint = Arc::new(checkpoint);

        self.send_request(WorkerRequest::AppendFile(FileEntry::new(
            offset.0,
            self.open.chunk.f.clone(),
            ChunkPersistedCallback::new(
                self.on_chunk_persisted.clone(),
                Some(checkpoint.clone()),
            ),
        )))?;

        let chunk = old_open.chunk;
        let closed_id = chunk.chunk_id();
        let closed = ClosedChunk::new(chunk, checkpoint.clone());
        self.closed.insert(closed_id, closed);
        Ok(Some(checkpoint.as_ref().clone()))
    }

    /// Loads a record from a closed chunk.
    ///
    /// # Arguments
    ///
    /// * `log_data` - Metadata about the log entry to load
    ///
    /// # Returns
    ///
    /// Returns the log payload if found
    ///
    /// # Errors
    ///
    /// Returns an IO error if the chunk is not found or reading fails
    pub fn load_record(
        &self,
        chunk_id: &ChunkId,
        segment: Segment,
    ) -> Result<WALRecord<W>, io::Error> {
        // All logs in the open chunk are served before this fallback.

        let record = {
            let closed = self.closed.get(chunk_id).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "Chunk not found: {}; when:(open cache-miss read)",
                        chunk_id
                    ),
                )
            })?;
            closed.chunk.read_record(segment)?
        };

        Ok(record)
    }
}

impl<W> WAL<WALRecord<W>> for ChunkedWal<W>
where W: WalTypes
{
    fn append(&mut self, rec: &WALRecord<W>) -> Result<(), io::Error> {
        self.open.append_record(rec)?;
        Ok(())
    }

    fn last_segment(&self) -> Segment {
        self.open.chunk.last_segment()
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::io::Seek;
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::mpsc::SyncSender;
    use std::sync::mpsc::sync_channel;

    use codeq::Decode;
    use codeq::Encode;
    use codeq::OffsetSize;

    use crate::Chunk;
    use crate::ChunkId;
    use crate::ChunkPersisted;
    use crate::ChunkPersistedFn;
    use crate::ChunkedWal;
    use crate::Config;
    use crate::Segment;
    use crate::StateMachine;
    use crate::WAL;
    use crate::WALRecord;
    use crate::WalTypes;

    const TEST_ACTION_TYPE: u32 = 1;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestAction(String);

    impl Encode for TestAction {
        fn encode<Wt: io::Write>(&self, mut w: Wt) -> Result<usize, io::Error> {
            let mut n = TEST_ACTION_TYPE.encode(&mut w)?;
            n += self.0.encode(&mut w)?;
            Ok(n)
        }

        fn type_id(&self) -> Option<u32> {
            Some(TEST_ACTION_TYPE)
        }
    }

    impl Decode for TestAction {
        fn decode<R: io::Read>(mut r: R) -> Result<Self, io::Error> {
            let type_id = u32::decode(&mut r)?;
            if type_id != TEST_ACTION_TYPE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected action type id {}", type_id),
                ));
            }

            Ok(Self(String::decode(&mut r)?))
        }
    }

    #[derive(Debug, Default, Clone, PartialEq, Eq)]
    struct TestWal;

    impl WalTypes for TestWal {
        type Action = TestAction;
        type Checkpoint = String;
        type Callback = SyncSender<Result<(), io::Error>>;
    }

    #[derive(Debug, Default)]
    struct TestStateMachine {
        values: Vec<String>,
    }

    impl StateMachine<TestWal> for TestStateMachine {
        type Error = io::Error;

        fn apply(
            &mut self,
            record: &WALRecord<TestWal>,
            _chunk_id: ChunkId,
            _global_segment: crate::Segment,
        ) -> Result<(), Self::Error> {
            match record {
                WALRecord::Action(v) => self.values.push(v.0.clone()),
                WALRecord::Checkpoint(checkpoint) => {
                    self.values = decode_checkpoint(checkpoint);
                }
            }

            Ok(())
        }

        fn checkpoint(&self) -> String {
            encode_checkpoint(&self.values)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct PersistedCall {
        starting_offset: u64,
        synced_offset: u64,
        checkpoint: Option<String>,
    }

    fn encode_checkpoint(values: &[String]) -> String {
        values.join(",")
    }

    fn decode_checkpoint(checkpoint: &str) -> Vec<String> {
        if checkpoint.is_empty() {
            return Vec::new();
        }

        checkpoint.split(',').map(str::to_string).collect()
    }

    fn action(value: &str) -> WALRecord<TestWal> {
        WALRecord::Action(TestAction(value.to_string()))
    }

    fn callback(
        calls: Arc<Mutex<Vec<PersistedCall>>>,
    ) -> ChunkPersistedFn<TestWal> {
        Arc::new(
            move |persisted: ChunkPersisted,
                  checkpoint: Option<Arc<String>>| {
                calls.lock().unwrap().push(PersistedCall {
                    starting_offset: persisted.starting_offset,
                    synced_offset: persisted.synced_offset,
                    checkpoint: checkpoint.as_deref().cloned(),
                });
            },
        )
    }

    fn open_wal(
        config: &Config,
        calls: Arc<Mutex<Vec<PersistedCall>>>,
    ) -> Result<(ChunkedWal<TestWal>, TestStateMachine), io::Error> {
        let mut sm = TestStateMachine::default();
        let wal = ChunkedWal::open(
            Arc::new(config.clone()),
            &mut sm,
            callback(calls),
        )?;

        Ok((wal, sm))
    }

    fn append_action(
        wal: &mut ChunkedWal<TestWal>,
        sm: &mut TestStateMachine,
        value: &str,
    ) -> Result<crate::Segment, io::Error> {
        let record = action(value);
        wal.append(&record)?;
        let segment = wal.last_segment();
        sm.apply(&record, wal.open.chunk.chunk_id(), segment)?;
        wal.try_close_full_chunk(sm)?;
        Ok(segment)
    }

    fn sync_flush(wal: &mut ChunkedWal<TestWal>) -> Result<(), io::Error> {
        let (tx, rx) = sync_channel(1);
        wal.send_pending(true, Some(tx))?;
        rx.recv()
            .map_err(|e| io::Error::other(format!("flush callback: {e}")))??;
        wal.wait_worker_idle();
        Ok(())
    }

    fn no_sync_flush(wal: &mut ChunkedWal<TestWal>) -> Result<(), io::Error> {
        let (tx, rx) = sync_channel(1);
        wal.send_pending(false, Some(tx))?;
        rx.recv()
            .map_err(|e| io::Error::other(format!("flush callback: {e}")))??;
        wal.wait_worker_idle();
        Ok(())
    }

    fn temp_config() -> (tempfile::TempDir, Config) {
        let td = tempfile::tempdir().unwrap();
        let config = Config::new(td.path().to_str().unwrap());
        (td, config)
    }

    fn records_in_chunk(
        config: &Config,
        chunk_id: ChunkId,
    ) -> Result<Vec<WALRecord<TestWal>>, io::Error> {
        Chunk::<WALRecord<TestWal>>::dump(config, chunk_id)?
            .into_iter()
            .map(|res| res.map(|(_, record)| record))
            .collect()
    }

    #[test]
    fn test_open_append_flush_reopen() -> Result<(), io::Error> {
        let (_td, config) = temp_config();

        {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let (mut wal, mut sm) = open_wal(&config, calls)?;

            append_action(&mut wal, &mut sm, "a")?;
            append_action(&mut wal, &mut sm, "b")?;
            append_action(&mut wal, &mut sm, "c")?;
            sync_flush(&mut wal)?;

            assert_eq!(vec!["a", "b", "c"], sm.values);
            assert!(wal.closed.is_empty());
            assert_eq!(4, wal.open.chunk.records_count());
            assert!(format!("{wal:?}").contains("ChunkedWal"));
        }

        {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let (wal, sm) = open_wal(&config, calls)?;

            assert_eq!(vec!["a", "b", "c"], sm.values);
            assert!(wal.closed.is_empty());
            assert_eq!(4, wal.open.chunk.records_count());
        }

        Ok(())
    }

    #[test]
    fn test_list_chunk_ids_ignores_invalid_file_names() -> Result<(), io::Error>
    {
        let (_td, config) = temp_config();
        std::fs::write(config.chunk_path(ChunkId(12)), [])?;
        std::fs::write(format!("{}/not-a-chunk", config.dir), [])?;

        let lock = ChunkedWal::<TestWal>::acquire_lock(&config)?;
        let chunk_ids = ChunkedWal::<TestWal>::load_chunk_ids(&config, &lock)?;

        assert_eq!(vec![ChunkId(12)], chunk_ids);
        Ok(())
    }

    #[test]
    fn test_rotate_chunk_writes_checkpoint() -> Result<(), io::Error> {
        let (_td, mut config) = temp_config();
        config.chunk_max_records = Some(3);

        let calls = Arc::new(Mutex::new(Vec::new()));
        let (mut wal, mut sm) = open_wal(&config, calls)?;

        append_action(&mut wal, &mut sm, "a")?;
        append_action(&mut wal, &mut sm, "b")?;
        append_action(&mut wal, &mut sm, "c")?;
        sync_flush(&mut wal)?;

        assert_eq!(1, wal.closed.len());
        assert_eq!(
            "a,b",
            wal.closed.first_key_value().unwrap().1.state.as_ref()
        );

        let records = records_in_chunk(&config, wal.open.chunk.chunk_id())?;
        assert_eq!(
            vec![WALRecord::Checkpoint("a,b".to_string()), action("c"),],
            records
        );

        Ok(())
    }

    #[test]
    fn test_reopen_reuses_last_healthy_chunk() -> Result<(), io::Error> {
        let (_td, mut config) = temp_config();
        config.chunk_max_records = Some(3);

        let open_chunk_id = {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let (mut wal, mut sm) = open_wal(&config, calls)?;

            for value in ["a", "b", "c", "d"] {
                append_action(&mut wal, &mut sm, value)?;
            }
            sync_flush(&mut wal)?;

            assert_eq!(2, wal.closed.len());
            wal.open.chunk.chunk_id()
        };

        let calls = Arc::new(Mutex::new(Vec::new()));
        let (wal, sm) = open_wal(&config, calls)?;

        assert_eq!(vec!["a", "b", "c", "d"], sm.values);
        assert_eq!(2, wal.closed.len());
        assert_eq!(open_chunk_id, wal.open.chunk.chunk_id());
        assert_eq!(1, wal.open.chunk.records_count());

        Ok(())
    }

    #[test]
    fn test_reopen_truncates_incomplete_last_record() -> Result<(), io::Error> {
        let (_td, config) = temp_config();

        let truncated_from = {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let (mut wal, mut sm) = open_wal(&config, calls)?;

            append_action(&mut wal, &mut sm, "a")?;
            append_action(&mut wal, &mut sm, "b")?;
            let segment = append_action(&mut wal, &mut sm, "c")?;
            sync_flush(&mut wal)?;

            let chunk_id = wal.open.chunk.chunk_id();
            let f = Chunk::<WALRecord<TestWal>>::open_chunk_file(
                &config, chunk_id,
            )?;
            let damaged_len = segment.end().0 - chunk_id.offset() - 1;
            f.set_len(damaged_len)?;
            damaged_len
        };

        let calls = Arc::new(Mutex::new(Vec::new()));
        let (wal, sm) = open_wal(&config, calls)?;

        assert_eq!(vec!["a", "b"], sm.values);
        assert_eq!(1, wal.closed.len());
        assert_eq!(
            Some(truncated_from),
            wal.last_closed_chunk_truncated_file_size()
        );
        assert_eq!(
            Some(truncated_from),
            wal.closed.first_key_value().unwrap().1.chunk.truncated_file_size()
        );
        assert_eq!(
            WALRecord::Checkpoint("a,b".to_string()),
            wal.open.chunk.read_record(wal.open.chunk.last_segment())?
        );

        Ok(())
    }

    #[test]
    fn test_reopen_truncates_trailing_zeroes() -> Result<(), io::Error> {
        let (_td, config) = temp_config();

        let original_len = {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let (mut wal, mut sm) = open_wal(&config, calls)?;

            append_action(&mut wal, &mut sm, "a")?;
            append_action(&mut wal, &mut sm, "b")?;
            sync_flush(&mut wal)?;

            let chunk_id = wal.open.chunk.chunk_id();
            let original_len = wal.open.chunk.global_end() - chunk_id.offset();
            let mut f = Chunk::<WALRecord<TestWal>>::open_chunk_file(
                &config, chunk_id,
            )?;
            f.seek(io::SeekFrom::Start(original_len))?;
            f.write_all(&[0, 0, 0])?;
            original_len
        };

        let calls = Arc::new(Mutex::new(Vec::new()));
        let (wal, sm) = open_wal(&config, calls)?;

        assert_eq!(vec!["a", "b"], sm.values);
        assert_eq!(1, wal.closed.len());
        assert_eq!(
            Some(original_len + 3),
            wal.last_closed_chunk_truncated_file_size()
        );
        assert_eq!(
            Some(original_len + 3),
            wal.closed.first_key_value().unwrap().1.chunk.truncated_file_size()
        );

        Ok(())
    }

    #[test]
    fn test_reopen_rejects_damaged_trailing_checkpoint() -> Result<(), io::Error>
    {
        let (_td, config) = temp_config();

        {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let (mut wal, mut sm) = open_wal(&config, calls)?;

            append_action(&mut wal, &mut sm, "a")?;
            append_action(&mut wal, &mut sm, "b")?;
            sync_flush(&mut wal)?;

            let chunk_id = wal.open.chunk.chunk_id();
            let original_len = wal.open.chunk.global_end() - chunk_id.offset();
            let mut f = Chunk::<WALRecord<TestWal>>::open_chunk_file(
                &config, chunk_id,
            )?;
            let mut damaged = Vec::new();
            WALRecord::<TestWal>::Checkpoint("bad".to_string())
                .encode(&mut damaged)?;
            *damaged.last_mut().unwrap() ^= 1;

            f.seek(io::SeekFrom::Start(original_len))?;
            f.write_all(&damaged)?;
        }

        let calls = Arc::new(Mutex::new(Vec::new()));
        let err = match open_wal(&config, calls) {
            Ok(_) => panic!("damaged checkpoint record must fail"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("decode Record at offset"));

        Ok(())
    }

    #[test]
    fn test_reopen_rejects_gap_between_chunks() -> Result<(), io::Error> {
        let (_td, mut config) = temp_config();
        config.chunk_max_records = Some(3);

        {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let (mut wal, mut sm) = open_wal(&config, calls)?;

            append_action(&mut wal, &mut sm, "a")?;
            let truncated_segment = append_action(&mut wal, &mut sm, "b")?;
            append_action(&mut wal, &mut sm, "c")?;
            sync_flush(&mut wal)?;

            let chunk_id = *wal.closed.first_key_value().unwrap().0;
            let f = Chunk::<WALRecord<TestWal>>::open_chunk_file(
                &config, chunk_id,
            )?;
            let truncated_len = truncated_segment.end().0 - chunk_id.offset();
            f.set_len(truncated_len - 1)?;
        }

        let calls = Arc::new(Mutex::new(Vec::new()));
        let err = open_wal(&config, calls).expect_err("chunk gap must fail");

        assert!(err.to_string().contains("Gap between chunks"));

        Ok(())
    }

    #[test]
    fn test_on_chunk_persisted_called_on_recovery() -> Result<(), io::Error> {
        let (_td, mut config) = temp_config();
        config.chunk_max_records = Some(3);

        {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let (mut wal, mut sm) = open_wal(&config, calls)?;

            for value in ["a", "b", "c", "d"] {
                append_action(&mut wal, &mut sm, value)?;
            }
            sync_flush(&mut wal)?;
        }

        let calls = Arc::new(Mutex::new(Vec::new()));
        let (_wal, sm) = open_wal(&config, calls.clone())?;

        assert_eq!(vec!["a", "b", "c", "d"], sm.values);
        assert_eq!(
            vec![None, Some("a,b".to_string()), Some("a,b,c,d".to_string()),],
            calls
                .lock()
                .unwrap()
                .iter()
                .map(|call| call.checkpoint.clone())
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    #[test]
    fn test_on_chunk_persisted_tracks_rotated_file() -> Result<(), io::Error> {
        let (_td, mut config) = temp_config();
        config.chunk_max_records = Some(3);

        let calls = Arc::new(Mutex::new(Vec::new()));
        let (mut wal, mut sm) = open_wal(&config, calls.clone())?;

        append_action(&mut wal, &mut sm, "a")?;
        append_action(&mut wal, &mut sm, "b")?;

        let open_start = wal.open.chunk.global_start();
        sync_flush(&mut wal)?;

        assert!(calls.lock().unwrap().contains(&PersistedCall {
            starting_offset: open_start,
            synced_offset: wal.open.chunk.global_end(),
            checkpoint: Some("a,b".to_string()),
        }));

        Ok(())
    }

    #[test]
    fn test_loaded_chunk_accessors() -> Result<(), io::Error> {
        let (_td, mut config) = temp_config();
        config.chunk_max_records = Some(3);

        let calls = Arc::new(Mutex::new(Vec::new()));
        let (mut wal, mut sm) = open_wal(&config, calls)?;

        let segment_a = append_action(&mut wal, &mut sm, "a")?;
        append_action(&mut wal, &mut sm, "b")?;
        append_action(&mut wal, &mut sm, "c")?;
        sync_flush(&mut wal)?;

        let open_chunk_id = wal.open_chunk_id();
        let closed_stats = wal.closed_chunk_stats();
        let open_stat = wal.open_chunk_stat(sm.checkpoint());

        assert_eq!(1, closed_stats.len());
        assert_eq!(ChunkId(0), closed_stats[0].chunk_id);
        assert_eq!(3, closed_stats[0].records_count);
        assert_eq!("a,b", closed_stats[0].log_state);
        assert_eq!(open_chunk_id, open_stat.chunk_id);
        assert_eq!(2, open_stat.records_count);
        assert_eq!("a,b,c", open_stat.log_state);
        assert_eq!(open_stat.global_end, wal.on_disk_size());
        assert_eq!(None, wal.last_closed_chunk_truncated_file_size());

        assert_eq!(
            action("a"),
            wal.closed_chunk_reader().read_record(ChunkId(0), segment_a)?
        );

        let err =
            wal.load_record(&ChunkId(999), Segment::new(999, 1)).unwrap_err();
        assert_eq!(io::ErrorKind::NotFound, err.kind());
        assert!(err.to_string().contains("Chunk not found"));

        let mut dumped = Vec::new();
        wal.dump_loaded_records(|chunk_id, index, res| {
            dumped.push((chunk_id, index, res.map(|(_segment, rec)| rec)?));
            Ok(())
        })?;

        assert_eq!(
            vec![
                (ChunkId(0), 0, WALRecord::Checkpoint(String::new())),
                (ChunkId(0), 1, action("a")),
                (ChunkId(0), 2, action("b")),
                (open_chunk_id, 0, WALRecord::Checkpoint("a,b".to_string())),
                (open_chunk_id, 1, action("c")),
            ],
            dumped
        );

        let drained =
            wal.drain_closed_chunks_while(|checkpoint| checkpoint == "a,b");
        assert_eq!(vec![ChunkId(0)], drained);
        assert!(wal.closed_chunk_stats().is_empty());

        let path = config.chunk_path(ChunkId(0));
        assert!(std::path::Path::new(&path).exists());
        wal.send_remove_chunks(drained)?;
        wal.wait_worker_idle();
        assert!(!std::path::Path::new(&path).exists());

        Ok(())
    }

    #[test]
    fn test_drain_closed_chunks_while_stops_at_first_unmatched()
    -> Result<(), io::Error> {
        let (_td, mut config) = temp_config();
        config.chunk_max_records = Some(3);

        let calls = Arc::new(Mutex::new(Vec::new()));
        let (mut wal, mut sm) = open_wal(&config, calls)?;

        for value in ["a", "b", "c", "d", "e"] {
            append_action(&mut wal, &mut sm, value)?;
        }
        sync_flush(&mut wal)?;

        let closed_before = wal
            .closed_chunk_stats()
            .into_iter()
            .map(|stat| (stat.chunk_id, stat.log_state))
            .collect::<Vec<_>>();
        assert_eq!(
            vec![
                (ChunkId(0), "a,b".to_string()),
                (ChunkId(34), "a,b,c,d".to_string()),
            ],
            closed_before
        );

        let drained =
            wal.drain_closed_chunks_while(|checkpoint| checkpoint == "a,b");
        assert_eq!(vec![ChunkId(0)], drained);

        let closed_after = wal
            .closed_chunk_stats()
            .into_iter()
            .map(|stat| (stat.chunk_id, stat.log_state))
            .collect::<Vec<_>>();
        assert_eq!(vec![(ChunkId(34), "a,b,c,d".to_string())], closed_after);

        Ok(())
    }

    #[test]
    fn test_lock_blocks_second_open_and_dump() -> Result<(), io::Error> {
        let (_td, config) = temp_config();

        let calls = Arc::new(Mutex::new(Vec::new()));
        let (wal, _sm) = open_wal(&config, calls.clone())?;

        let err = ChunkedWal::<TestWal>::acquire_lock(&config)
            .expect_err("second lock must fail");
        assert_eq!(io::ErrorKind::WouldBlock, err.kind());

        drop(wal);

        let lock = ChunkedWal::<TestWal>::acquire_lock(&config)?;
        let mut records = Vec::new();
        ChunkedWal::<TestWal>::dump_records(
            &config,
            &lock,
            |chunk_id, i, res| {
                records.push((chunk_id, i, res.map(|(_, record)| record)?));
                Ok(())
            },
        )?;

        assert_eq!(
            vec![(ChunkId(0), 0, WALRecord::Checkpoint(String::new()))],
            records
        );

        Ok(())
    }

    #[test]
    fn test_flush_without_sync_writes_without_advancing_sync_id()
    -> Result<(), io::Error> {
        let (_td, config) = temp_config();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (mut wal, mut sm) = open_wal(&config, calls)?;

        append_action(&mut wal, &mut sm, "a")?;
        append_action(&mut wal, &mut sm, "b")?;
        no_sync_flush(&mut wal)?;

        assert_eq!(
            vec![(0, 0)],
            wal.get_stat()?
                .iter()
                .map(|stat| stat.offset_sync_id())
                .collect::<Vec<_>>()
        );

        sync_flush(&mut wal)?;

        assert!(
            wal.get_stat()?
                .iter()
                .any(|stat| stat.sync_id == wal.open.chunk.global_end())
        );

        Ok(())
    }
}
