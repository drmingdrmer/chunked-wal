use std::fs::File;
use std::io;
use std::io::IoSlice;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;
use std::time::Instant;

use log::debug;
use log::info;

use crate::WalTypes;
use crate::wal::atomic_flush_metrics::AtomicFlushMetrics;
use crate::wal::batch_metrics::BatchMetrics;
use crate::wal::callback::Callback;
use crate::wal::file_entry::FileEntry;
use crate::wal::file_persisted::ChunkPersisted;
use crate::wal::flush_request::FlushStat;
use crate::wal::flush_request::SeqRequest;
use crate::wal::flush_request::WorkerRequest;
use crate::wal::queued_write::QueuedWrite;
use crate::wal::write_batch::WriteBatch;

pub(crate) struct FlushWorker<W>
where W: WalTypes
{
    rx: Receiver<SeqRequest<W>>,
    files: Vec<FileEntry<W>>,
    metrics: Arc<AtomicFlushMetrics>,
    flush_batch_wait: Duration,
    flush_batch_max_items: usize,
    /// The highest completed request sequence number.
    ///
    /// Updated (with `Relaxed` ordering) after processing each request or
    /// batch. The main thread polls this to implement `wait_worker_idle()`.
    /// `Relaxed` is sufficient because this value is only a progress counter;
    /// request side effects provide their own synchronization.
    done_seq: Arc<AtomicU64>,
}

impl<W> FlushWorker<W>
where W: WalTypes
{
    /// When starting, there is at most one open chunk file that is not sync.
    pub(crate) fn spawn(self) {
        std::thread::Builder::new()
            .name("chunked_wal_flush_worker".to_string())
            .spawn(move || {
                self.run();
            })
            .expect("Failed to start sync worker thread");
    }

    pub(crate) fn new(
        rx: Receiver<SeqRequest<W>>,
        file_entry: FileEntry<W>,
        done_seq: Arc<AtomicU64>,
        metrics: Arc<AtomicFlushMetrics>,
        flush_batch_wait: Duration,
        flush_batch_max_items: usize,
    ) -> Self {
        Self {
            rx,
            files: vec![file_entry],
            metrics,
            flush_batch_wait,
            flush_batch_max_items,
            done_seq,
        }
    }

    fn run(self) {
        let res = self.run_inner();
        if let Err(e) = res {
            log::error!("FlushWorker failed: {}", e);
        }
    }

    fn run_inner(mut self) -> Result<(), io::Error> {
        loop {
            // Write requests should be batched to maximize throughput.
            let mut batch = WriteBatch::new(self.flush_batch_max_items);

            let req = self.rx.recv();
            let Ok(seq_req) = req else {
                log::info!("FlushWorker input channel closed, quit");
                return Ok(());
            };

            if !batch.push_seq_request(seq_req) {
                let Some(SeqRequest { seq, req, .. }) =
                    batch.last_non_flush.take()
                else {
                    unreachable!("non-write request must be stored");
                };
                self.handle_non_flush_request(req)?;
                self.done_seq.store(seq, Ordering::Relaxed);
                continue;
            }

            let group_wait = self.collect_write_batch(&mut batch);

            debug!("batched write: {}", batch.writes.len());

            let sync_result = {
                // TODO: possible to use write_all_vectored()?

                let mut last_file: &File = &self.files.last().unwrap().f;
                let batch_start = Instant::now();
                let mut batch_metrics =
                    BatchMetrics::new(batch.writes.len(), group_wait);
                for w in &batch.writes {
                    batch_metrics.record_queued_write(batch_start, w);
                }

                let write_start = Instant::now();
                write_batch_vectored(&mut last_file, &batch.writes)?;
                batch_metrics.record_write_time(write_start);

                let need_sync = batch.writes.iter().any(|w| w.write.sync);

                let sync_result = if need_sync {
                    let upto_offset =
                        batch.writes.last().unwrap().write.upto_offset;
                    let sync_start = Instant::now();
                    let res = self.sync_data_files(upto_offset);
                    batch_metrics.record_sync_time(sync_start);
                    if let Err(ref e) = res {
                        log::error!(
                            "Failed to flush upto offset {}: {}",
                            upto_offset,
                            e
                        );
                    }
                    res
                } else {
                    Ok(())
                };

                batch_metrics.record_batch_time(batch_start);
                self.metrics.record_batch(batch_metrics);

                sync_result
            };

            let WriteBatch {
                writes,
                mut max_seq,
                last_non_flush,
                ..
            } = batch;

            for w in writes {
                if let Some(cb) = w.write.callback {
                    match &sync_result {
                        Ok(()) => cb.send(Ok(())),
                        Err(e) => {
                            cb.send(Err(io::Error::new(
                                e.kind(),
                                e.to_string(),
                            )));
                        }
                    }
                }
            }

            // Handle the last non-flush request
            if let Some(SeqRequest {
                seq: nf_seq,
                req: last,
                ..
            }) = last_non_flush
            {
                self.handle_non_flush_request(last)?;
                max_seq = max_seq.max(nf_seq);
            }

            self.done_seq.store(max_seq, Ordering::Relaxed);
        }
    }

    fn collect_write_batch(&self, batch: &mut WriteBatch<W>) -> Duration {
        let loop_started_at = Instant::now();
        let loop_deadline = loop_started_at + self.flush_batch_wait;

        while batch.last_non_flush.is_none()
            && batch.writes.len() < batch.max_size
        {
            let now = Instant::now();
            if loop_deadline <= now {
                break;
            }

            let remaining = loop_deadline - now;
            match self.rx.recv_timeout(remaining) {
                Ok(seq_req) => {
                    if !batch.push_seq_request(seq_req) {
                        break;
                    }
                }
                Err(
                    RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected,
                ) => break,
            }
        }

        loop_started_at.elapsed()
    }

    fn handle_non_flush_request(
        &mut self,
        req: WorkerRequest<W>,
    ) -> Result<(), io::Error> {
        match req {
            WorkerRequest::AppendFile(file_entry) => {
                info!("FlushWorker: AppendFile: {}", file_entry);
                self.files.push(file_entry);
            }
            WorkerRequest::Write(_) => {
                unreachable!("Write request should be handled in run()");
            }
            WorkerRequest::GetFlushStat { tx } => {
                let stat = self
                    .files
                    .iter()
                    .map(|f| FlushStat {
                        starting_offset: f.starting_offset,
                        sync_id: f.sync_id,
                        ino: f.f.metadata().unwrap().ino(),
                    })
                    .collect();
                let _ = tx.send(stat);
            }
            WorkerRequest::RemoveChunks { chunk_paths } => {
                info!("FlushWorker: RemoveChunks: {:?}", chunk_paths);
                for path in chunk_paths {
                    std::fs::remove_file(path)?;
                }
            }
        }

        Ok(())
    }

    pub fn sync_data_files(&mut self, offset: u64) -> Result<(), io::Error> {
        let files = &mut self.files;

        if files.is_empty() {
            return Ok(());
        }

        // Append-only WAL flushes only need file data durability. Metadata
        // changes such as recovery truncation are synchronized at the call
        // site that performs the metadata update.
        while files.len() > 1 {
            let f = files.remove(0);
            f.f.sync_data()?;
        }

        let f = &mut files[0];
        f.f.sync_data()?;
        f.on_persisted.call(ChunkPersisted {
            file: f.f.clone(),
            starting_offset: f.starting_offset,
            synced_offset: offset,
        });
        f.sync_id = offset;

        Ok(())
    }
}

fn write_batch_vectored<W>(
    file: &mut &File,
    writes: &[QueuedWrite<W>],
) -> Result<(), io::Error>
where
    W: WalTypes,
{
    const MAX_VECTORED_WRITE_SLICES: usize = 1024;

    for chunk in writes.chunks(MAX_VECTORED_WRITE_SLICES) {
        let mut slices = chunk
            .iter()
            .filter(|w| !w.write.data.is_empty())
            .map(|w| w.write.data.as_slice())
            .collect::<Vec<_>>();

        if !slices.is_empty() {
            write_all_vectored(file, &mut slices)?;
        }
    }

    Ok(())
}

fn write_all_vectored(
    file: &mut &File,
    buffers: &mut [&[u8]],
) -> Result<(), io::Error> {
    let mut start = 0;

    while start < buffers.len() {
        let io_slices = buffers[start..]
            .iter()
            .map(|buffer| IoSlice::new(buffer))
            .collect::<Vec<_>>();

        let mut written = match file.write_vectored(&io_slices) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };

        while written > 0 {
            let len = buffers[start].len();
            if written < len {
                buffers[start] = &buffers[start][written..];
                break;
            }

            written -= len;
            start += 1;
            if start == buffers.len() {
                break;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::sync::mpsc::Receiver;
    use std::sync::mpsc::SyncSender;
    use std::sync::mpsc::sync_channel;
    use std::time::Duration;
    use std::time::Instant;

    use crate::WalTypes;
    use crate::wal::atomic_flush_metrics::AtomicFlushMetrics;
    use crate::wal::flush_request::SeqRequest;
    use crate::wal::flush_request::WorkerRequest;
    use crate::wal::flush_request::WriteRequest;
    use crate::wal::flush_worker::FlushWorker;
    use crate::wal::write_batch::WriteBatch;

    #[derive(Debug, Default, Clone, PartialEq, Eq)]
    struct TestWal;

    impl WalTypes for TestWal {
        type Action = String;
        type Checkpoint = String;
        type Callback = SyncSender<Result<(), io::Error>>;
    }

    fn worker(
        rx: Receiver<SeqRequest<TestWal>>,
        flush_batch_wait: Duration,
    ) -> FlushWorker<TestWal> {
        FlushWorker {
            rx,
            files: Vec::new(),
            metrics: Arc::new(AtomicFlushMetrics::default()),
            flush_batch_wait,
            flush_batch_max_items: 8,
            done_seq: Arc::new(AtomicU64::new(0)),
        }
    }

    fn write_request(seq: u64) -> SeqRequest<TestWal> {
        SeqRequest {
            seq,
            queued_at: Instant::now(),
            req: WorkerRequest::Write(WriteRequest {
                upto_offset: seq,
                data: vec![seq as u8],
                sync: false,
                callback: None,
            }),
        }
    }

    #[test]
    fn test_sync_data_files_empty_file_list() -> Result<(), io::Error> {
        let (_tx, rx) = sync_channel(1);
        let mut worker = worker(rx, Duration::ZERO);

        worker.sync_data_files(10)?;

        assert!(worker.files.is_empty());
        Ok(())
    }

    #[test]
    fn test_collect_write_batch_stops_at_deadline() {
        let (_tx, rx) = sync_channel(1);
        let worker = worker(rx, Duration::ZERO);
        let mut batch = WriteBatch::new(8);
        assert!(batch.push_seq_request(write_request(1)));

        worker.collect_write_batch(&mut batch);

        assert_eq!(1, batch.writes.len());
        assert_eq!(1, batch.max_seq);
        assert!(batch.last_non_flush.is_none());
    }

    #[test]
    fn test_collect_write_batch_stops_at_non_write_request() {
        let (tx, rx) = sync_channel(1);
        tx.send(SeqRequest {
            seq: 2,
            queued_at: Instant::now(),
            req: WorkerRequest::RemoveChunks {
                chunk_paths: vec!["obsolete".to_string()],
            },
        })
        .unwrap();

        let worker = worker(rx, Duration::from_secs(1));
        let mut batch = WriteBatch::new(8);
        assert!(batch.push_seq_request(write_request(1)));

        worker.collect_write_batch(&mut batch);

        assert_eq!(1, batch.writes.len());
        assert_eq!(1, batch.max_seq);
        let last = batch.last_non_flush.unwrap();
        assert_eq!(2, last.seq);
        assert!(matches!(
            last.req,
            WorkerRequest::RemoveChunks { chunk_paths }
                if chunk_paths == vec!["obsolete".to_string()]
        ));
    }
}
