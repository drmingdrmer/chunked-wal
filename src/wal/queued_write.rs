use std::time::Instant;

use crate::WalTypes;
use crate::wal::flush_request::WriteRequest;

pub(crate) struct QueuedWrite<W>
where W: WalTypes
{
    pub(crate) queued_at: Instant,
    pub(crate) write: WriteRequest<W>,
}
