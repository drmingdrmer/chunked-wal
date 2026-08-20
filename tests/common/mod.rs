//! A minimal WAL type set shared by the integration tests.
//!
//! The tests drive the crate only through its public API, so each one needs
//! its own action, checkpoint, and state machine.

use std::io;
use std::sync::Arc;
use std::sync::mpsc::SyncSender;
use std::sync::mpsc::sync_channel;

use chunked_wal::ChunkId;
use chunked_wal::ChunkPersistedFn;
use chunked_wal::ChunkedWal;
use chunked_wal::Config;
use chunked_wal::Segment;
use chunked_wal::StateMachine;
use chunked_wal::WAL;
use chunked_wal::WALRecord;
use chunked_wal::WalTypes;
use codeq::Decode;
use codeq::Encode;

pub const TEST_ACTION_TYPE: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestAction(pub String);

impl Encode for TestAction {
    fn encode<W: io::Write>(&self, mut w: W) -> Result<usize, io::Error> {
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
pub struct TestWal;

impl WalTypes for TestWal {
    type Action = TestAction;
    type Checkpoint = String;
    type Callback = SyncSender<Result<(), io::Error>>;
}

/// Rebuilds the list of applied action values.
///
/// The checkpoint is the comma-joined value list, so replaying from any
/// chunk's checkpoint reproduces the same state.
#[derive(Debug, Default)]
pub struct TestStateMachine {
    pub values: Vec<String>,
}

impl StateMachine<TestWal> for TestStateMachine {
    type Error = io::Error;

    fn apply(
        &mut self,
        record: &WALRecord<TestWal>,
        _chunk_id: ChunkId,
        _global_segment: Segment,
    ) -> Result<(), Self::Error> {
        match record {
            WALRecord::Action(action) => self.values.push(action.0.clone()),
            WALRecord::Checkpoint(checkpoint) => {
                self.values = decode_checkpoint(checkpoint);
            }
        }

        Ok(())
    }

    fn checkpoint(&self) -> String {
        self.values.join(",")
    }
}

pub fn decode_checkpoint(checkpoint: &str) -> Vec<String> {
    if checkpoint.is_empty() {
        return Vec::new();
    }

    checkpoint.split(',').map(str::to_string).collect()
}

pub fn ignore_persisted() -> ChunkPersistedFn<TestWal> {
    Arc::new(|_persisted, _checkpoint| {})
}

pub fn open_wal(
    config: &Config,
) -> Result<(ChunkedWal<TestWal>, TestStateMachine), io::Error> {
    let mut sm = TestStateMachine::default();
    let wal = ChunkedWal::open(
        Arc::new(config.clone()),
        &mut sm,
        ignore_persisted(),
    )?;

    Ok((wal, sm))
}

/// Appends one action, applies it, and rotates the chunk when it is full.
pub fn append_action(
    wal: &mut ChunkedWal<TestWal>,
    sm: &mut TestStateMachine,
    value: &str,
) -> Result<Segment, io::Error> {
    let record = WALRecord::Action(TestAction(value.to_string()));
    wal.append(&record)?;

    let segment = wal.last_segment();
    sm.apply(&record, wal.open_chunk_id(), segment)?;
    wal.try_close_full_chunk(sm)?;

    Ok(segment)
}

/// Hands the pending records to the flush worker and waits for its callback.
pub fn flush(
    wal: &mut ChunkedWal<TestWal>,
    sync: bool,
) -> Result<(), io::Error> {
    let (tx, rx) = sync_channel(1);
    wal.send_pending(sync, Some(tx))?;
    rx.recv()
        .map_err(|e| io::Error::other(format!("flush callback: {e}")))??;
    wal.wait_worker_idle()
}
