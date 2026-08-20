use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::sync::Arc;
use std::sync::mpsc::SyncSender;

use codeq::OffsetSize;
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
use crate::wal::closed_chunk_reader::ClosedChunkReader;
use crate::wal::file_entry::FileEntry;
use crate::wal::file_persisted::ChunkPersisted;
use crate::wal::file_persisted::ChunkPersistedCallback;
use crate::wal::file_persisted::ChunkPersistedFn;
use crate::wal::flush_client::FlushClient;
use crate::wal::flush_request::FlushStat;
use crate::wal::flush_request::WorkerRequest;
use crate::wal::flush_request::WriteRequest;
use crate::wal::flush_worker::FlushWorker;
use crate::wal::flush_worker::WorkerState;

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
    flush_client: FlushClient<W>,

    /// File-level callback invoked after fsync.
    ///
    /// This callback is called once for each synced chunk file.
    on_chunk_persisted: ChunkPersistedFn<W>,

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
            .field("sent_seq", &self.flush_client.sent_seq())
            .field("done_seq", &self.flush_client.done_seq())
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
        let mut chunk_ids = Self::load_chunk_ids(&config, &dir_lock)?;
        Self::remove_incomplete_tail_chunks(&config, &mut chunk_ids)?;

        let mut closed = BTreeMap::new();
        let mut prev_end_offset = None;
        let mut prev_checkpoint = None;
        let tail_chunk_id = chunk_ids.last().copied();

        for chunk_id in chunk_ids.iter().copied() {
            Self::ensure_consecutive_chunks(prev_end_offset, chunk_id)?;

            let (chunk, records) = Chunk::<WALRecord<W>>::open_with_truncate(
                config.clone(),
                chunk_id,
                Some(chunk_id) == tail_chunk_id,
            )?;

            on_chunk_persisted(
                ChunkPersisted {
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
            OpenChunk::create_with_initial_record(
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

        let worker_state = Arc::new(WorkerState::new());
        let flush_metrics = Arc::new(AtomicFlushMetrics::default());

        let (flush_tx, rx) = std::sync::mpsc::sync_channel(1024);
        let worker = FlushWorker::new(
            rx,
            file_entry,
            worker_state.clone(),
            flush_metrics.clone(),
            config.clone(),
        );

        worker.spawn();

        let flush_client = FlushClient::new(flush_tx, worker_state);

        Self {
            config,
            open,
            closed,
            flush_client,
            on_chunk_persisted,
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

    /// Removes successor files left uncommitted by interrupted rotation.
    ///
    /// The worker writes a successor's first record only after syncing its
    /// predecessor, so a trailing chunk without that record can be discarded.
    fn remove_incomplete_tail_chunks(
        config: &Config,
        chunk_ids: &mut Vec<ChunkId>,
    ) -> Result<(), io::Error> {
        loop {
            let Some(chunk_id) = chunk_ids.last().copied() else {
                return Ok(());
            };

            if Self::has_complete_initial_record(config, chunk_id)? {
                return Ok(());
            }

            log::warn!(
                "Removing trailing chunk without complete initial record: \
                 {chunk_id}"
            );

            let path = config.chunk_path(chunk_id);
            std::fs::remove_file(path)?;
            config.sync_dir()?;

            chunk_ids.pop();
        }
    }

    fn has_complete_initial_record(
        config: &Config,
        chunk_id: ChunkId,
    ) -> Result<bool, io::Error> {
        let file = Chunk::<WALRecord<W>>::open_chunk_file(config, chunk_id)?;
        let mut records = Chunk::<WALRecord<W>>::load_records_iter(
            config,
            Arc::new(file),
            chunk_id,
        )?;

        let Some(first) = records.next() else {
            return Ok(false);
        };

        match first {
            Ok(_) => Ok(true),
            Err(error) => {
                if error.kind() == io::ErrorKind::UnexpectedEof {
                    Ok(false)
                } else {
                    Err(error)
                }
            }
        }
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
        self.flush_client.send(req)
    }

    /// Block until the FlushWorker has processed all requests sent so far.
    pub fn wait_worker_idle(&self) -> Result<(), io::Error> {
        self.flush_client.wait_idle()
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

        self.send_pending(true, None)?;

        let config = self.config.clone();
        let offset = self.open.chunk.last_segment().end();

        info!(
            "Closing full chunk: {}, open new: {}",
            self.open.chunk.chunk_id(),
            ChunkId(offset.0)
        );

        let checkpoint = state_machine.checkpoint();
        let checkpoint = Arc::new(checkpoint);

        let new_open = {
            let chunk_id = ChunkId(offset.0);
            OpenChunk::<WALRecord<W>>::create_empty(config, chunk_id)?
        };

        self.send_request(WorkerRequest::AppendFile(FileEntry::new(
            offset.0,
            new_open.chunk.f.clone(),
            ChunkPersistedCallback::new(
                self.on_chunk_persisted.clone(),
                Some(checkpoint.clone()),
            ),
        )))?;

        let old_open = std::mem::replace(&mut self.open, new_open);

        self.append(&WALRecord::Checkpoint(checkpoint.as_ref().clone()))?;
        // FIFO writes the checkpoint after the predecessor sync. Until then,
        // recovery treats this successor as uncommitted.
        self.send_pending(false, None)?;

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
mod tests;
