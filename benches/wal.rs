//! Release-mode microbenchmarks for the WAL's public API.
//!
//! Run with `make bench` (or `cargo bench`). Every case prints one line:
//! the case name, the operation count, the wall time, and the per-operation
//! cost. Numbers are host-specific; use them to compare a change against the
//! same host's previous run, not as absolute figures.
//!
//! The `sync_latency` and `group_commit` cases differ only in whether the
//! caller waits for each durable request before sending the next. Comparing
//! them shows what cross-request group commit is worth: both perform the same
//! number of durable appends, but `group_commit` collapses them into far
//! fewer `sync_data` calls.

use std::io;
use std::sync::Arc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::SyncSender;
use std::sync::mpsc::sync_channel;
use std::time::Duration;
use std::time::Instant;

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

const ACTION_TYPE: u32 = 1;

/// A payload-sized action: a 4-byte type id, a `u64` id, and opaque bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BenchAction {
    id: u64,
    payload: Vec<u8>,
}

impl Encode for BenchAction {
    fn encode<W: io::Write>(&self, mut w: W) -> Result<usize, io::Error> {
        let mut n = ACTION_TYPE.encode(&mut w)?;
        n += self.id.encode(&mut w)?;
        n += self.payload.encode(&mut w)?;
        Ok(n)
    }

    fn type_id(&self) -> Option<u32> {
        Some(ACTION_TYPE)
    }
}

impl Decode for BenchAction {
    fn decode<R: io::Read>(mut r: R) -> Result<Self, io::Error> {
        let type_id = u32::decode(&mut r)?;
        if type_id != ACTION_TYPE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected action type id {}", type_id),
            ));
        }

        Ok(Self {
            id: u64::decode(&mut r)?,
            payload: Vec::<u8>::decode(&mut r)?,
        })
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct BenchWal;

impl WalTypes for BenchWal {
    type Action = BenchAction;
    type Checkpoint = u64;
    type Callback = SyncSender<Result<(), io::Error>>;
}

/// Counts applied records; the checkpoint is the last applied action id.
#[derive(Debug, Default)]
struct BenchStateMachine {
    last_id: u64,
    applied: u64,
}

impl StateMachine<BenchWal> for BenchStateMachine {
    type Error = io::Error;

    fn apply(
        &mut self,
        record: &WALRecord<BenchWal>,
        _chunk_id: ChunkId,
        _global_segment: Segment,
    ) -> Result<(), Self::Error> {
        match record {
            WALRecord::Action(action) => {
                self.last_id = action.id;
                self.applied += 1;
            }
            WALRecord::Checkpoint(last_id) => self.last_id = *last_id,
        }

        Ok(())
    }

    fn checkpoint(&self) -> u64 {
        self.last_id
    }
}

fn action(id: u64, payload_size: usize) -> WALRecord<BenchWal> {
    WALRecord::Action(BenchAction {
        id,
        payload: vec![(id % 251) as u8; payload_size],
    })
}

fn open(config: &Config) -> (ChunkedWal<BenchWal>, BenchStateMachine) {
    let on_persisted: ChunkPersistedFn<BenchWal> = Arc::new(|_p, _c| {});
    let mut sm = BenchStateMachine::default();
    let wal = ChunkedWal::open(Arc::new(config.clone()), &mut sm, on_persisted)
        .expect("open");

    (wal, sm)
}

/// Appends one record and rotates the chunk when it is full.
fn append(
    wal: &mut ChunkedWal<BenchWal>,
    sm: &mut BenchStateMachine,
    id: u64,
    payload_size: usize,
) -> Segment {
    let record = action(id, payload_size);
    wal.append(&record).expect("append");

    let segment = wal.last_segment();
    sm.apply(&record, wal.open_chunk_id(), segment).expect("apply");
    wal.try_close_full_chunk(sm).expect("rotate");

    segment
}

fn send_sync(
    wal: &mut ChunkedWal<BenchWal>,
) -> Receiver<Result<(), io::Error>> {
    let (tx, rx) = sync_channel(1);
    wal.send_pending(true, Some(tx)).expect("send_pending");
    rx
}

fn await_callback(rx: &Receiver<Result<(), io::Error>>) {
    rx.recv().expect("callback channel").expect("durable write");
}

fn report(case: &str, operations: usize, elapsed: Duration, note: &str) {
    let per_op = elapsed.as_secs_f64() / operations as f64;
    let per_sec = operations as f64 / elapsed.as_secs_f64();

    println!(
        "{case:<16} {operations:>8} ops  {:>9.3} ms total  \
         {:>10.3} us/op  {per_sec:>12.0} ops/s  {note}",
        elapsed.as_secs_f64() * 1000.0,
        per_op * 1_000_000.0,
    );
}

fn temp_config() -> (tempfile::TempDir, Config) {
    let td = tempfile::tempdir().expect("temp dir");
    let config = Config::new(td.path().to_str().unwrap());
    (td, config)
}

/// One durable append at a time: every request waits for its own `sync_data`.
fn bench_sync_latency() {
    const OPERATIONS: usize = 200;
    const PAYLOAD: usize = 128;

    let (_td, config) = temp_config();
    let (mut wal, mut sm) = open(&config);

    let started_at = Instant::now();
    for id in 0..OPERATIONS as u64 {
        append(&mut wal, &mut sm, id, PAYLOAD);
        let rx = send_sync(&mut wal);
        await_callback(&rx);
    }
    let elapsed = started_at.elapsed();

    let metrics = wal.flush_metrics();
    report(
        "sync_latency",
        OPERATIONS,
        elapsed,
        &format!("{} sync batches", metrics.sync_batch_count),
    );
    wal.shutdown().expect("shutdown");
}

/// Durable appends issued without waiting, so the worker groups them.
fn bench_group_commit() {
    const OPERATIONS: usize = 4096;
    const PAYLOAD: usize = 128;

    let (_td, config) = temp_config();
    let (mut wal, mut sm) = open(&config);

    let started_at = Instant::now();
    let mut callbacks = Vec::with_capacity(OPERATIONS);
    for id in 0..OPERATIONS as u64 {
        append(&mut wal, &mut sm, id, PAYLOAD);
        callbacks.push(send_sync(&mut wal));
    }
    for rx in &callbacks {
        await_callback(rx);
    }
    let elapsed = started_at.elapsed();

    let metrics = wal.flush_metrics();
    report(
        "group_commit",
        OPERATIONS,
        elapsed,
        &format!(
            "{} sync batches, max batch {}",
            metrics.sync_batch_count, metrics.batch_size_max
        ),
    );
    wal.shutdown().expect("shutdown");
}

/// Appends across a chunk size small enough to rotate repeatedly.
fn bench_rollover() {
    const OPERATIONS: usize = 4096;
    const PAYLOAD: usize = 1024;
    const CHUNK_MAX_SIZE: usize = 64 * 1024;

    let (_td, mut config) = temp_config();
    config.chunk_max_size = Some(CHUNK_MAX_SIZE);
    let (mut wal, mut sm) = open(&config);

    let started_at = Instant::now();
    for id in 0..OPERATIONS as u64 {
        append(&mut wal, &mut sm, id, PAYLOAD);
        if id % 64 == 63 {
            let rx = send_sync(&mut wal);
            await_callback(&rx);
        }
    }
    let rx = send_sync(&mut wal);
    await_callback(&rx);
    let elapsed = started_at.elapsed();

    let rotations = wal.closed_chunk_stats().len();
    report(
        "rollover",
        OPERATIONS,
        elapsed,
        &format!("{rotations} chunks closed"),
    );
    wal.shutdown().expect("shutdown");
}

/// Reopens a populated directory and replays it into the state machine.
fn bench_replay() {
    const OPERATIONS: usize = 8192;
    const PAYLOAD: usize = 1024;

    let (_td, config) = temp_config();
    {
        let (mut wal, mut sm) = open(&config);
        for id in 0..OPERATIONS as u64 {
            append(&mut wal, &mut sm, id, PAYLOAD);
            if id % 64 == 63 {
                wal.send_pending(false, None).expect("send_pending");
            }
        }
        wal.shutdown().expect("shutdown");
    }

    let started_at = Instant::now();
    let (mut wal, sm) = open(&config);
    let elapsed = started_at.elapsed();

    assert_eq!(OPERATIONS as u64, sm.applied);
    report("replay", OPERATIONS, elapsed, "records replayed on open");
    wal.shutdown().expect("shutdown");
}

/// Reads records from closed chunks in a scattered order.
fn bench_random_reads() {
    const OPERATIONS: usize = 8192;
    const PAYLOAD: usize = 1024;
    const CHUNK_MAX_RECORDS: usize = 512;

    let (_td, mut config) = temp_config();
    config.chunk_max_records = Some(CHUNK_MAX_RECORDS);
    let (mut wal, mut sm) = open(&config);

    let mut located = Vec::with_capacity(OPERATIONS);
    for id in 0..OPERATIONS as u64 {
        let chunk_id = wal.open_chunk_id();
        let segment = append(&mut wal, &mut sm, id, PAYLOAD);
        located.push((chunk_id, segment));
    }
    let rx = send_sync(&mut wal);
    await_callback(&rx);

    // Records still in the open chunk are not readable through the closed
    // chunk reader, so read only the ones a rotation has closed.
    let open_chunk_id = wal.open_chunk_id();
    located.retain(|(chunk_id, _)| *chunk_id != open_chunk_id);
    let reader = wal.closed_chunk_reader();

    // A multiplicative step coprime with the length visits every record in a
    // scattered order without needing a random number generator.
    const STEP: usize = 7919;
    let started_at = Instant::now();
    for i in 0..located.len() {
        let (chunk_id, segment) = located[(i * STEP) % located.len()];
        reader.read_record(chunk_id, segment).expect("read_record");
    }
    let elapsed = started_at.elapsed();

    report("random_reads", located.len(), elapsed, "closed-chunk reads");
    wal.shutdown().expect("shutdown");
}

/// Durable appends of records far larger than one page.
fn bench_large_records() {
    const OPERATIONS: usize = 32;
    const PAYLOAD: usize = 4 * 1024 * 1024;

    let (_td, config) = temp_config();
    let (mut wal, mut sm) = open(&config);

    let started_at = Instant::now();
    for id in 0..OPERATIONS as u64 {
        append(&mut wal, &mut sm, id, PAYLOAD);
        let rx = send_sync(&mut wal);
        await_callback(&rx);
    }
    let elapsed = started_at.elapsed();

    let megabytes = (OPERATIONS * PAYLOAD) as f64 / (1024.0 * 1024.0);
    let throughput = megabytes / elapsed.as_secs_f64();
    report(
        "large_records",
        OPERATIONS,
        elapsed,
        &format!("{PAYLOAD} B each, {throughput:.0} MiB/s"),
    );
    wal.shutdown().expect("shutdown");
}

fn main() {
    println!("chunked-wal benchmarks");

    bench_sync_latency();
    bench_group_commit();
    bench_rollover();
    bench_replay();
    bench_random_reads();
    bench_large_records();
}
