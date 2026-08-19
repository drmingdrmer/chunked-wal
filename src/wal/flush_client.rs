use std::fs::File;
use std::io;
use std::sync::Arc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::SyncSender;
use std::time::Instant;

use crate::WalTypes;
use crate::wal::flush_request::CreateChunkRequest;
use crate::wal::flush_request::SeqRequest;
use crate::wal::flush_request::WorkerRequest;
use crate::wal::flush_worker::WorkerState;

/// Sends sequenced requests to the flush worker and tracks its progress.
pub(super) struct FlushClient<W>
where W: WalTypes
{
    /// Channel for sending ordered requests to the flush worker.
    tx: SyncSender<SeqRequest<W>>,

    /// Sequence number assigned to the most recent request.
    sent_seq: u64,

    /// Shared completion and failure state reported by the flush worker.
    state: Arc<WorkerState>,
}

impl<W> FlushClient<W>
where W: WalTypes
{
    /// Creates a client with no requests assigned.
    pub(super) fn new(
        tx: SyncSender<SeqRequest<W>>,
        state: Arc<WorkerState>,
    ) -> Self {
        Self {
            tx,
            sent_seq: 0,
            state,
        }
    }

    /// Assigns and returns the next sequence after sending the request.
    pub(super) fn send(
        &mut self,
        req: WorkerRequest<W>,
    ) -> Result<u64, io::Error> {
        let seq = self.sent_seq + 1;
        self.tx
            .send(SeqRequest {
                seq,
                queued_at: Instant::now(),
                req,
            })
            .map_err(|e| io::Error::other(format!("send request: {e}")))?;
        self.sent_seq = seq;
        Ok(seq)
    }

    /// Creates a chunk through the worker and returns its file handle.
    pub(super) fn create_chunk(
        &mut self,
        request: CreateChunkRequest<W>,
        result_rx: Receiver<Result<Arc<File>, io::Error>>,
    ) -> Result<Arc<File>, io::Error> {
        let seq = self.send(WorkerRequest::CreateChunk(request))?;
        match result_rx.recv() {
            Ok(result) => result,
            Err(err) => {
                if let Some(failure) = self.state.failure() {
                    return Err(failure);
                }
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    format!("receive created chunk: {err}; request: {seq}"),
                ))
            }
        }
    }

    /// Waits until the worker reaches the current sequence or reports failure.
    pub(super) fn wait_idle(&self) -> Result<(), io::Error> {
        self.state.wait_for(self.sent_seq)
    }

    /// Returns the sequence number assigned to the most recent request.
    pub(super) fn sent_seq(&self) -> u64 {
        self.sent_seq
    }

    /// Returns the highest sequence number completed by the worker.
    pub(super) fn done_seq(&self) -> u64 {
        self.state.done_seq()
    }
}
