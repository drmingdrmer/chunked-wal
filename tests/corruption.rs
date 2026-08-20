//! Single-bit corruption of a WAL directory.
//!
//! Record integrity is split between two layers. The WAL owns the checkpoint
//! that opens every chunk and checksums it, so damage there is its own to
//! detect. An action's bytes belong to the application's codec: the WAL
//! frames the record but does not checksum its contents, and an application
//! that wants that guarantee puts a checksum in its own encoding.
//!
//! These tests pin both halves of that split, then walk every bit of every
//! chunk file to check that recovery neither panics nor invents records.

mod common;

use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use chunked_wal::ChunkId;
use chunked_wal::ChunkedWal;
use chunked_wal::Config;
use codeq::OffsetSize;
use common::TestWal;
use common::append_action;
use common::flush;
use common::open_wal;

/// Three values and a three-record chunk limit put one action in the tail
/// chunk: chunk 0 holds the first checkpoint plus `alpha` and `beta`, and the
/// tail holds a checkpoint plus `gamma`.
///
/// Corrupting a *closed* chunk's action would be invisible, because the next
/// chunk's checkpoint replaces the state those actions built. Only the tail
/// chunk's actions still shape the recovered state.
const VALUES: [&str; 3] = ["alpha", "beta", "gamma"];

/// The last value in [`VALUES`], which lives in the tail chunk.
const TAIL_VALUE: &str = "gamma";

/// One chunk file, its uncorrupted contents, and its checkpoint's extent.
struct ChunkImage {
    path: PathBuf,
    bytes: Vec<u8>,

    /// Byte length of the chunk's initial checkpoint record.
    ///
    /// Every chunk starts with one, and the WAL checksums it.
    checkpoint_len: usize,
}

/// Writes a WAL holding [`VALUES`] across several chunks and returns its files.
fn build_wal_image(dir: &Path) -> Result<Vec<ChunkImage>, io::Error> {
    let config = image_config(dir);

    let (mut wal, mut sm) = open_wal(&config)?;
    for value in VALUES {
        append_action(&mut wal, &mut sm, value)?;
    }
    flush(&mut wal, true)?;
    wal.shutdown()?;
    // `shutdown` joins the flush worker; the directory lock is only released
    // when the WAL itself is dropped.
    drop(wal);

    let checkpoint_lens = checkpoint_lengths(&config)?;

    let images = chunk_paths(dir)?
        .into_iter()
        .map(|path| {
            let bytes = std::fs::read(&path)?;
            let chunk_id = chunk_id_of(&path);
            let checkpoint_len = checkpoint_lens[&chunk_id];
            Ok(ChunkImage {
                path,
                bytes,
                checkpoint_len,
            })
        })
        .collect::<Result<Vec<_>, io::Error>>()?;

    assert!(images.len() >= 2, "the image must span several chunks");
    Ok(images)
}

fn image_config(dir: &Path) -> Config {
    let mut config = Config::new(dir.to_str().unwrap());
    config.chunk_max_records = Some(3);
    config
}

/// Returns each chunk's initial checkpoint record length, by chunk id.
fn checkpoint_lengths(
    config: &Config,
) -> Result<BTreeMap<ChunkId, usize>, io::Error> {
    let lock = ChunkedWal::<TestWal>::acquire_lock(config)?;
    let mut lengths = BTreeMap::new();

    ChunkedWal::<TestWal>::dump_records(
        config,
        &lock,
        |chunk_id, index, res| {
            let (segment, _record) = res?;
            if index == 0 {
                lengths.insert(chunk_id, *segment.size() as usize);
            }
            Ok(())
        },
    )?;

    Ok(lengths)
}

fn chunk_id_of(path: &Path) -> ChunkId {
    let name = path.file_name().unwrap().to_string_lossy().to_string();
    let offset = Config::parse_chunk_file_name(&name).expect("chunk name");
    ChunkId(offset)
}

/// Returns the chunk files in `dir`, sorted by name.
fn chunk_paths(dir: &Path) -> Result<Vec<PathBuf>, io::Error> {
    let mut paths = Vec::new();

    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if name.starts_with("r-") && name.ends_with(".wal") {
            paths.push(path);
        }
    }

    paths.sort();
    Ok(paths)
}

/// Restores every chunk file, flipping one bit of `corrupted`.
///
/// A `corrupted` index outside `images` restores the pristine files.
fn restore_with_flipped_bit(
    dir: &Path,
    images: &[ChunkImage],
    corrupted: usize,
    byte_index: usize,
    bit: u32,
) -> Result<(), io::Error> {
    // Recovery may delete a chunk file, so clear whatever the last open left.
    for path in chunk_paths(dir)? {
        std::fs::remove_file(path)?;
    }

    for (i, image) in images.iter().enumerate() {
        let mut bytes = image.bytes.clone();
        if i == corrupted {
            bytes[byte_index] ^= 1 << bit;
        }
        std::fs::write(&image.path, &bytes)?;
    }

    Ok(())
}

/// Replays the WAL in `dir` without letting recovery truncate a damaged tail.
fn replay_without_truncation(dir: &Path) -> Result<Vec<String>, io::Error> {
    let mut config = image_config(dir);
    config.truncate_incomplete_record = Some(false);

    let (mut wal, sm) = open_wal(&config)?;
    wal.shutdown()?;

    Ok(sm.values)
}

#[test]
fn test_intact_image_replays_every_record() -> Result<(), io::Error> {
    let td = tempfile::tempdir()?;
    let images = build_wal_image(td.path())?;

    // The same restore path without a mutation, so a failure below is
    // attributable to the flipped bit rather than to rebuilding the files.
    restore_with_flipped_bit(td.path(), &images, usize::MAX, 0, 0)?;

    assert_eq!(VALUES.to_vec(), replay_without_truncation(td.path())?);
    Ok(())
}

/// The WAL owns the checkpoint, so it must catch every bit flipped in one.
///
/// Only closed chunks are walked: a damaged checkpoint in the tail chunk is
/// indistinguishable from an interrupted rotation, which recovery is entitled
/// to resolve by deleting that chunk rather than by failing.
#[test]
fn test_corrupting_a_closed_chunk_checkpoint_fails_the_open()
-> Result<(), io::Error> {
    let td = tempfile::tempdir()?;
    let images = build_wal_image(td.path())?;
    let tail = images.len() - 1;

    let mut mutations = 0;

    for corrupted in 0..tail {
        for byte_index in 0..images[corrupted].checkpoint_len {
            for bit in 0..8 {
                restore_with_flipped_bit(
                    td.path(),
                    &images,
                    corrupted,
                    byte_index,
                    bit,
                )?;
                mutations += 1;

                let replayed = replay_without_truncation(td.path());
                assert!(
                    replayed.is_err(),
                    "flipping bit {bit} of byte {byte_index} in the \
                     checkpoint of closed chunk {corrupted} opened \
                     successfully as {:?}",
                    replayed.unwrap()
                );
            }
        }
    }

    let expected_mutations: usize =
        images[..tail].iter().map(|i| i.checkpoint_len * 8).sum();
    assert_eq!(expected_mutations, mutations);

    Ok(())
}

/// An action's bytes are the application codec's responsibility.
///
/// This pins the documented split: the WAL replays a corrupted action without
/// complaint, because the checkpoint checksum does not extend over actions.
/// An application that needs the guarantee checksums its own encoding.
#[test]
fn test_corrupting_an_action_payload_is_not_detected() -> Result<(), io::Error>
{
    let td = tempfile::tempdir()?;
    let images = build_wal_image(td.path())?;
    let tail = images.len() - 1;

    // Search past the checkpoint: the same text also appears in a later
    // chunk's checkpoint, where the WAL's own checksum would catch the flip.
    let checkpoint_len = images[tail].checkpoint_len;
    let action_bytes = &images[tail].bytes[checkpoint_len..];
    let payload_offset = action_bytes
        .windows(TAIL_VALUE.len())
        .position(|window| window == TAIL_VALUE.as_bytes())
        .expect("the tail chunk must hold the last action's payload");
    let payload_start = checkpoint_len + payload_offset;

    restore_with_flipped_bit(td.path(), &images, tail, payload_start, 0)?;

    let mut corrupted_value = TAIL_VALUE.as_bytes().to_vec();
    corrupted_value[0] ^= 1;
    let mut expected = VALUES.map(str::to_string).to_vec();
    *expected.last_mut().unwrap() = String::from_utf8(corrupted_value).unwrap();

    assert_eq!(expected, replay_without_truncation(td.path())?);
    Ok(())
}

/// Recovery must survive every single-bit mutation of the whole directory.
///
/// The assertion is deliberately weak because action bytes are unprotected:
/// what the WAL owes here is that a damaged file never panics recovery and
/// never yields more records than were written.
#[test]
fn test_single_bit_corruption_never_panics_or_invents_records()
-> Result<(), io::Error> {
    let td = tempfile::tempdir()?;
    let images = build_wal_image(td.path())?;

    let mut mutations = 0;
    let mut rejected = 0;

    for corrupted in 0..images.len() {
        for byte_index in 0..images[corrupted].bytes.len() {
            for bit in 0..8 {
                restore_with_flipped_bit(
                    td.path(),
                    &images,
                    corrupted,
                    byte_index,
                    bit,
                )?;
                mutations += 1;

                let Ok(replayed) = replay_without_truncation(td.path()) else {
                    rejected += 1;
                    continue;
                };

                assert!(
                    replayed.len() <= VALUES.len(),
                    "flipping bit {bit} of byte {byte_index} in chunk \
                     {corrupted} replayed {replayed:?}, more records than \
                     the {} written",
                    VALUES.len()
                );
            }
        }
    }

    let total_bytes: usize = images.iter().map(|i| i.bytes.len()).sum();
    assert_eq!(total_bytes * 8, mutations);
    assert!(rejected > 0, "no mutation at all was rejected");

    Ok(())
}
