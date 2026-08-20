use std::io;
use std::sync::Arc;
use std::sync::mpsc::SyncSender;
use std::time::Instant;

use crate::WalTypes;
use crate::wal::flush_request::SeqRequest;
use crate::wal::flush_request::WorkerRequest;
use crate::wal::flush_worker::WorkerState;

/// Sends sequenced requests to the flush worker and tracks its progress.
pub(super) struct FlushClient<W>
where W: WalTypes
{
    /// Channel for sending ordered requests to the flush worker.
    ///
    /// `None` after [`FlushClient::close`], which rejects further requests.
    tx: Option<SyncSender<SeqRequest<W>>>,

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
            tx: Some(tx),
            sent_seq: 0,
            state,
        }
    }

    /// Assigns the next sequence number and sends the request to the worker.
    pub(super) fn send(
        &mut self,
        req: WorkerRequest<W>,
    ) -> Result<(), io::Error> {
        let Some(tx) = self.tx.as_ref() else {
            return Err(io::Error::other(
                "Failed to send request: WAL is shut down",
            ));
        };

        // Reserve the request's memory before queueing it, so a slow worker
        // throttles the sender instead of letting queued data grow without
        // bound.
        let reserved = match &req {
            WorkerRequest::Write(write) => write.data.len(),
            _ => 0,
        };
        self.state.queued_bytes().acquire(reserved);

        self.sent_seq += 1;
        let res = tx
            .send(SeqRequest {
                seq: self.sent_seq,
                queued_at: Instant::now(),
                req,
            })
            .map_err(|e| {
                io::Error::other(format!("Failed to send request: {}", e))
            });

        if res.is_err() {
            self.state.queued_bytes().release(reserved);
        }

        res
    }

    /// Rejects further requests and lets the worker stop once its queue is
    /// drained.
    pub(super) fn close(&mut self) {
        self.tx = None;
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
