use crate::WalTypes;
use crate::wal::flush_request::SeqRequest;
use crate::wal::flush_request::WorkerRequest;
use crate::wal::queued_write::QueuedWrite;

pub(crate) struct WriteBatch<W>
where W: WalTypes
{
    pub(crate) writes: Vec<QueuedWrite<W>>,
    pub(crate) max_seq: u64,
    pub(crate) last_non_flush: Option<SeqRequest<W>>,
    pub(crate) max_size: usize,
}

impl<W> WriteBatch<W>
where W: WalTypes
{
    pub(crate) fn new(max_size: usize) -> Self {
        Self {
            writes: Vec::with_capacity(max_size),
            max_seq: 0,
            last_non_flush: None,
            max_size,
        }
    }

    pub(crate) fn push_seq_request(&mut self, seq_req: SeqRequest<W>) -> bool {
        let SeqRequest {
            seq,
            queued_at,
            req,
        } = seq_req;

        if let WorkerRequest::Write(write) = req {
            self.max_seq = self.max_seq.max(seq);
            self.writes.push(QueuedWrite { queued_at, write });
            true
        } else {
            self.last_non_flush = Some(SeqRequest {
                seq,
                queued_at,
                req,
            });
            false
        }
    }
}
