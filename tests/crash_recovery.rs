//! Recovery after a process dies without running destructors.
//!
//! The in-process tests can only build a crash *image* by hand. These tests
//! run the WAL in a child process that leaves through `std::process::exit`,
//! so no `Drop` runs: the flush worker is not joined, pending records are not
//! written, and the directory lock is released only by the kernel. The parent
//! then reopens the same directory and checks what survived.

mod common;

use std::io;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use chunked_wal::ChunkedWal;
use chunked_wal::Config;
use common::TestWal;
use common::append_action;
use common::flush;
use common::ignore_persisted;
use common::open_wal;

/// Names the point at which the child process must leave.
const CRASH_POINT_ENV: &str = "CHUNKED_WAL_CRASH_POINT";

/// Directory the child process opens its WAL in.
const CRASH_DIR_ENV: &str = "CHUNKED_WAL_CRASH_DIR";

const CHILD_VALUES: [&str; 2] = ["a", "b"];

/// Runs the child half of a crash test and never returns.
///
/// Appends [`CHILD_VALUES`], carries them as far as `crash_point` asks, then
/// terminates the process without unwinding.
fn crash_child(crash_point: &str) -> ! {
    let dir = std::env::var(CRASH_DIR_ENV).expect("crash dir must be set");
    let config = Config::new(dir);

    let (mut wal, mut sm) = open_wal(&config).expect("child must open the WAL");
    for value in CHILD_VALUES {
        append_action(&mut wal, &mut sm, value).expect("child must append");
    }

    match crash_point {
        // Nothing left `OpenChunk::pending_data`.
        "buffered" => {}
        // The worker completed the write syscall but no fsync.
        "written" => flush(&mut wal, false).expect("child must write"),
        // The worker completed the write and the fsync.
        "synced" => flush(&mut wal, true).expect("child must sync"),
        other => panic!("unknown crash point: {other}"),
    }

    std::process::exit(0);
}

/// Returns the crash point when this process is the child of a crash test.
fn crash_point() -> Option<String> {
    std::env::var(CRASH_POINT_ENV).ok()
}

/// Runs `test_name` again in a child process that crashes at `crash_point`.
fn spawn_crashing_child(dir: &Path, test_name: &str, crash_point: &str) {
    let exe = std::env::current_exe().expect("test binary path");

    let status = Command::new(exe)
        .args(["--exact", test_name])
        .env(CRASH_POINT_ENV, crash_point)
        .env(CRASH_DIR_ENV, dir)
        .status()
        .expect("child process must start");

    assert!(status.success(), "child exited with {status}");
}

/// Reopens the crashed directory and returns the replayed action values.
fn replay(dir: &Path) -> Result<Vec<String>, io::Error> {
    let config = Config::new(dir.to_str().unwrap());
    let (mut wal, sm) = open_wal(&config)?;
    wal.shutdown()?;

    Ok(sm.values)
}

#[test]
fn test_crash_after_sync_replays_every_record() -> Result<(), io::Error> {
    if let Some(point) = crash_point() {
        crash_child(&point);
    }

    let td = tempfile::tempdir()?;
    spawn_crashing_child(
        td.path(),
        "test_crash_after_sync_replays_every_record",
        "synced",
    );

    assert_eq!(CHILD_VALUES.to_vec(), replay(td.path())?);
    Ok(())
}

#[test]
fn test_crash_after_write_replays_every_record() -> Result<(), io::Error> {
    if let Some(point) = crash_point() {
        crash_child(&point);
    }

    let td = tempfile::tempdir()?;
    spawn_crashing_child(
        td.path(),
        "test_crash_after_write_replays_every_record",
        "written",
    );

    // The bytes reached the OS, so they outlive the process even without an
    // fsync. Only a machine-level failure could lose them here.
    assert_eq!(CHILD_VALUES.to_vec(), replay(td.path())?);
    Ok(())
}

#[test]
fn test_crash_before_write_loses_buffered_records() -> Result<(), io::Error> {
    if let Some(point) = crash_point() {
        crash_child(&point);
    }

    let td = tempfile::tempdir()?;
    spawn_crashing_child(
        td.path(),
        "test_crash_before_write_loses_buffered_records",
        "buffered",
    );

    // `append` only encodes into memory, so a crash before the flush loses
    // the records. The WAL must still open and replay its checkpoint.
    assert_eq!(Vec::<String>::new(), replay(td.path())?);
    Ok(())
}

#[test]
fn test_crashed_directory_lock_is_reusable() -> Result<(), io::Error> {
    if let Some(point) = crash_point() {
        crash_child(&point);
    }

    let td = tempfile::tempdir()?;
    spawn_crashing_child(
        td.path(),
        "test_crashed_directory_lock_is_reusable",
        "synced",
    );

    // The child never released the lock; the kernel did when it exited.
    let config = Arc::new(Config::new(td.path().to_str().unwrap()));
    let mut sm = common::TestStateMachine::default();
    let mut wal =
        ChunkedWal::<TestWal>::open(config, &mut sm, ignore_persisted())?;
    wal.shutdown()?;

    assert_eq!(CHILD_VALUES.to_vec(), sm.values);
    Ok(())
}
