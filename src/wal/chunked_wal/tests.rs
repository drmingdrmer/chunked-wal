use std::io;
use std::io::Seek;
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::SyncSender;
use std::sync::mpsc::sync_channel;
use std::time::Duration;

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
        move |persisted: ChunkPersisted, checkpoint: Option<Arc<String>>| {
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
    let wal =
        ChunkedWal::open(Arc::new(config.clone()), &mut sm, callback(calls))?;

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
    wal.wait_worker_idle()?;
    Ok(())
}

fn no_sync_flush(wal: &mut ChunkedWal<TestWal>) -> Result<(), io::Error> {
    let (tx, rx) = sync_channel(1);
    wal.send_pending(false, Some(tx))?;
    rx.recv()
        .map_err(|e| io::Error::other(format!("flush callback: {e}")))??;
    wal.wait_worker_idle()?;
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
fn test_list_chunk_ids_ignores_invalid_file_names() -> Result<(), io::Error> {
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
fn test_reopen_recovers_crash_before_predecessor_sync() -> Result<(), io::Error>
{
    let (_td, mut config) = temp_config();
    config.chunk_max_records = Some(3);

    // Pause the worker after syncing the durable prefix. The next rotation
    // can then create its successor before writing the predecessor's tail.
    let (reached_tx, reached_rx) = sync_channel(1);
    let (resume_tx, resume_rx) = sync_channel(1);
    let gate = Arc::new(Mutex::new(Some((reached_tx, resume_rx))));
    let on_chunk_persisted: ChunkPersistedFn<TestWal> = Arc::new({
        let gate = gate.clone();
        move |_persisted, _checkpoint| {
            let pending = {
                let mut gate = gate.lock().unwrap();
                gate.take()
            };
            let Some((reached_tx, resume_rx)) = pending else {
                return;
            };
            if reached_tx.send(()).is_ok() {
                let _ = resume_rx.recv();
            }
        }
    });

    let mut sm = TestStateMachine::default();
    let mut wal = ChunkedWal::open(
        Arc::new(config.clone()),
        &mut sm,
        on_chunk_persisted,
    )?;
    append_action(&mut wal, &mut sm, "durable")?;
    wal.send_pending(true, None)?;
    reached_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|error| io::Error::other(error.to_string()))?;

    // Rotate while the worker is blocked, producing the original failure
    // state: the predecessor ends before the empty successor's chunk ID.
    append_action(&mut wal, &mut sm, "not-durable")?;
    let predecessor_id = *wal.closed.last_key_value().unwrap().0;
    let successor_id = wal.open_chunk_id();
    let predecessor_size =
        std::fs::metadata(config.chunk_path(predecessor_id))?.len();
    let predecessor_end = predecessor_id.offset() + predecessor_size;
    assert!(predecessor_end < successor_id.offset());
    assert_eq!(0, std::fs::metadata(config.chunk_path(successor_id))?.len());

    // Preserve the inconsistent files as a crash image before allowing the
    // original worker to finish its queued writes.
    let (_crash_td, mut crash_config) = temp_config();
    crash_config.truncate_incomplete_record = Some(false);
    for chunk_id in [predecessor_id, successor_id] {
        std::fs::copy(
            config.chunk_path(chunk_id),
            crash_config.chunk_path(chunk_id),
        )?;
    }

    resume_tx.send(()).map_err(|error| io::Error::other(error.to_string()))?;
    wal.wait_worker_idle()?;

    // Recovery must remove the empty successor and replay only the exact
    // durable prefix from its predecessor.
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (crash_wal, crash_sm) = open_wal(&crash_config, calls)?;
    assert_eq!(vec!["durable"], crash_sm.values);
    assert_eq!(predecessor_id, crash_wal.open_chunk_id());
    drop(crash_wal);

    let lock = ChunkedWal::<TestWal>::acquire_lock(&crash_config)?;
    let chunk_ids =
        ChunkedWal::<TestWal>::load_chunk_ids(&crash_config, &lock)?;
    assert_eq!(vec![predecessor_id], chunk_ids);
    Ok(())
}

#[test]
fn test_reopen_removes_incomplete_tail_chunks() -> Result<(), io::Error> {
    let (_td, mut config) = temp_config();
    config.truncate_incomplete_record = Some(false);

    let first_tail_id = {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (mut wal, mut sm) = open_wal(&config, calls)?;
        append_action(&mut wal, &mut sm, "a")?;
        sync_flush(&mut wal)?;
        ChunkId(wal.open.chunk.global_end())
    };

    let mut checkpoint = Vec::new();
    WALRecord::<TestWal>::Checkpoint("a".to_string())
        .encode(&mut checkpoint)?;
    let second_tail_id =
        ChunkId(first_tail_id.offset() + checkpoint.len() as u64);
    checkpoint.pop();

    std::fs::write(config.chunk_path(first_tail_id), checkpoint)?;
    std::fs::write(config.chunk_path(second_tail_id), [])?;

    let calls = Arc::new(Mutex::new(Vec::new()));
    let (wal, sm) = open_wal(&config, calls)?;
    assert_eq!(vec!["a"], sm.values);
    assert_eq!(ChunkId(0), wal.open_chunk_id());
    drop(wal);

    let lock = ChunkedWal::<TestWal>::acquire_lock(&config)?;
    let chunk_ids = ChunkedWal::<TestWal>::load_chunk_ids(&config, &lock)?;
    assert_eq!(vec![ChunkId(0)], chunk_ids);
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
        let f =
            Chunk::<WALRecord<TestWal>>::open_chunk_file(&config, chunk_id)?;
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
        let mut f =
            Chunk::<WALRecord<TestWal>>::open_chunk_file(&config, chunk_id)?;
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
fn test_reopen_rejects_damaged_trailing_checkpoint() -> Result<(), io::Error> {
    let (_td, config) = temp_config();

    {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (mut wal, mut sm) = open_wal(&config, calls)?;

        append_action(&mut wal, &mut sm, "a")?;
        append_action(&mut wal, &mut sm, "b")?;
        sync_flush(&mut wal)?;

        let chunk_id = wal.open.chunk.chunk_id();
        let original_len = wal.open.chunk.global_end() - chunk_id.offset();
        let mut f =
            Chunk::<WALRecord<TestWal>>::open_chunk_file(&config, chunk_id)?;
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
fn test_reopen_rejects_damaged_non_tail_chunk_without_truncating()
-> Result<(), io::Error> {
    let (_td, mut config) = temp_config();
    config.chunk_max_records = Some(3);

    let (chunk_id, damaged_len) = {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (mut wal, mut sm) = open_wal(&config, calls)?;

        append_action(&mut wal, &mut sm, "a")?;
        let truncated_segment = append_action(&mut wal, &mut sm, "b")?;
        append_action(&mut wal, &mut sm, "c")?;
        sync_flush(&mut wal)?;

        let chunk_id = *wal.closed.first_key_value().unwrap().0;
        let f =
            Chunk::<WALRecord<TestWal>>::open_chunk_file(&config, chunk_id)?;
        let truncated_len = truncated_segment.end().0 - chunk_id.offset();
        let damaged_len = truncated_len - 1;
        f.set_len(damaged_len)?;
        (chunk_id, damaged_len)
    };

    let calls = Arc::new(Mutex::new(Vec::new()));
    let err =
        open_wal(&config, calls).expect_err("damaged non-tail chunk must fail");

    assert!(err.to_string().contains("decode Record at offset"));

    let f = Chunk::<WALRecord<TestWal>>::open_chunk_file(&config, chunk_id)?;
    assert_eq!(damaged_len, f.metadata()?.len());

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

    let err = wal.load_record(&ChunkId(999), Segment::new(999, 1)).unwrap_err();
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
    wal.wait_worker_idle()?;
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
    ChunkedWal::<TestWal>::dump_records(&config, &lock, |chunk_id, i, res| {
        records.push((chunk_id, i, res.map(|(_, record)| record)?));
        Ok(())
    })?;

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

#[test]
fn test_bounded_flush_queue_persists_every_record() -> Result<(), io::Error> {
    let (_td, mut config) = temp_config();
    // One byte forces every send to wait for the worker to drain the queue,
    // so a byte reservation the worker forgets to release would deadlock.
    config.flush_queue_max_bytes = Some(1);

    let expected = (0..64).map(|i| format!("v{i}")).collect::<Vec<String>>();

    {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (mut wal, mut sm) = open_wal(&config, calls)?;

        for value in &expected {
            append_action(&mut wal, &mut sm, value)?;
            wal.send_pending(false, None)?;
        }

        sync_flush(&mut wal)?;
    }

    let calls = Arc::new(Mutex::new(Vec::new()));
    let (_wal, sm) = open_wal(&config, calls)?;
    assert_eq!(expected, sm.values);

    Ok(())
}

#[test]
fn test_writes_ignore_a_moved_file_cursor() -> Result<(), io::Error> {
    let (_td, config) = temp_config();

    {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (mut wal, mut sm) = open_wal(&config, calls)?;

        append_action(&mut wal, &mut sm, "a")?;
        sync_flush(&mut wal)?;

        // The flush worker shares this file description, so rewinding it
        // would send the next write to the start of the chunk.
        let mut writer_file: &std::fs::File = &wal.open.chunk.f;
        writer_file.seek(io::SeekFrom::Start(0))?;

        append_action(&mut wal, &mut sm, "b")?;
        sync_flush(&mut wal)?;
    }

    let calls = Arc::new(Mutex::new(Vec::new()));
    let (_wal, sm) = open_wal(&config, calls)?;
    assert_eq!(vec!["a", "b"], sm.values);

    Ok(())
}

#[test]
fn test_worker_failure_wakes_waiter_and_fails_later_waits()
-> Result<(), io::Error> {
    let (_td, config) = temp_config();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (mut wal, _sm) = open_wal(&config, calls)?;

    wal.send_remove_chunks(vec![ChunkId(999)])?;

    let err = wal.wait_worker_idle().unwrap_err();
    assert_eq!(io::ErrorKind::NotFound, err.kind());

    let res = wal.send_remove_chunks(vec![ChunkId(999)]);
    if let Err(err) = res {
        assert_eq!(io::ErrorKind::Other, err.kind());
    }

    let err = wal.wait_worker_idle().unwrap_err();
    assert_eq!(io::ErrorKind::NotFound, err.kind());

    Ok(())
}
