use std::format;
use std::io;
use std::time::Duration;

use crate::ChunkId;
use crate::errors::InvalidChunkFileName;
use crate::num;

/// Buffer size for the sequential read that replays one chunk.
const DEFAULT_READ_BUFFER_SIZE: usize = 64 * 1024 * 1024;

const DEFAULT_FLUSH_BATCH_WAIT: Duration = Duration::from_millis(1);
const DEFAULT_FLUSH_BATCH_MAX_ITEMS: usize = 2048;
const DEFAULT_FLUSH_QUEUE_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Largest accepted `read_buffer_size`.
///
/// The buffer only serves a sequential replay of one chunk, so a value this
/// large is already far past useful and is more likely a unit mistake.
const MAX_READ_BUFFER_SIZE: usize = 1024 * 1024 * 1024;

/// Configuration for chunked WAL.
///
/// This struct holds directory, chunk, recovery, and flush batching settings.
///
/// Optional parameters are `Option<T>` in this struct, and default values is
/// evaluated when a getter method is called.
#[derive(Clone, Debug, Default)]
pub struct Config {
    /// Base directory for storing WAL files.
    pub dir: String,

    /// Size of the read buffer in bytes.
    pub read_buffer_size: Option<usize>,

    /// Maximum number of records in a chunk.
    pub chunk_max_records: Option<usize>,

    /// Maximum size of a chunk in bytes.
    pub chunk_max_size: Option<usize>,

    /// Whether to truncate the last half sync-ed record.
    ///
    /// If truncate, the chunk is considered successfully opened.
    /// Otherwise, an io::Error will be returned.
    pub truncate_incomplete_record: Option<bool>,

    /// Maximum time the flush worker waits for more write requests before
    /// starting a sync batch.
    ///
    /// Defaults to 1 millisecond.
    pub flush_batch_wait: Option<Duration>,

    /// Maximum number of write requests to include in one flush batch.
    ///
    /// Defaults to 2048.
    pub flush_batch_max_items: Option<usize>,

    /// Maximum number of bytes of queued write requests held in memory.
    ///
    /// A request that would exceed this limit blocks its sender until the
    /// flush worker has written earlier requests. Defaults to 64MB. One
    /// request bigger than the whole limit is still accepted, once nothing
    /// else is queued.
    pub flush_queue_max_bytes: Option<usize>,
}

impl Config {
    /// Creates a new Config with the specified directory and defaults.
    pub fn new(dir: impl ToString) -> Self {
        Self {
            dir: dir.to_string(),
            ..Default::default()
        }
    }

    /// Creates a new Config with all configurable parameters
    pub fn new_full(
        dir: impl ToString,
        read_buffer_size: Option<usize>,
        chunk_max_records: Option<usize>,
        chunk_max_size: Option<usize>,
    ) -> Self {
        Self {
            dir: dir.to_string(),
            read_buffer_size,
            chunk_max_records,
            chunk_max_size,
            truncate_incomplete_record: None,
            flush_batch_wait: None,
            flush_batch_max_items: None,
            flush_queue_max_bytes: None,
        }
    }

    /// Returns the size of read buffer in bytes (defaults to 64MB)
    pub fn read_buffer_size(&self) -> usize {
        self.read_buffer_size.unwrap_or(DEFAULT_READ_BUFFER_SIZE)
    }

    /// Returns the maximum number of records per chunk (defaults to 1M records)
    pub fn chunk_max_records(&self) -> usize {
        self.chunk_max_records.unwrap_or(1024 * 1024)
    }

    /// Returns the maximum size of a chunk in bytes (defaults to 1GB)
    pub fn chunk_max_size(&self) -> usize {
        self.chunk_max_size.unwrap_or(1024 * 1024 * 1024)
    }

    /// Returns whether to truncate incomplete records (defaults to true)
    pub fn truncate_incomplete_record(&self) -> bool {
        self.truncate_incomplete_record.unwrap_or(true)
    }

    /// Returns the bounded wait before syncing a flush batch.
    pub fn flush_batch_wait(&self) -> Duration {
        self.flush_batch_wait.unwrap_or(DEFAULT_FLUSH_BATCH_WAIT)
    }

    /// Returns the maximum number of write requests in one flush batch.
    pub fn flush_batch_max_items(&self) -> usize {
        self.flush_batch_max_items.unwrap_or(DEFAULT_FLUSH_BATCH_MAX_ITEMS)
    }

    /// Returns the maximum bytes of queued write requests held in memory.
    pub fn flush_queue_max_bytes(&self) -> usize {
        self.flush_queue_max_bytes.unwrap_or(DEFAULT_FLUSH_QUEUE_MAX_BYTES)
    }

    /// Rejects settings that cannot produce a working WAL.
    ///
    /// Opening or dumping a WAL validates first, so a bad value fails there
    /// instead of turning into a silent empty read, a chunk that is full the
    /// moment it is created, or a flush batch that holds nothing.
    pub fn validate(&self) -> Result<(), io::Error> {
        if self.dir.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Config::dir is empty",
            ));
        }

        Self::reject_zero("read_buffer_size", self.read_buffer_size)?;
        Self::reject_zero("chunk_max_records", self.chunk_max_records)?;
        Self::reject_zero("chunk_max_size", self.chunk_max_size)?;
        Self::reject_zero("flush_batch_max_items", self.flush_batch_max_items)?;
        Self::reject_zero("flush_queue_max_bytes", self.flush_queue_max_bytes)?;

        let read_buffer_size = self.read_buffer_size();
        if read_buffer_size > MAX_READ_BUFFER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Config::read_buffer_size {} exceeds the {} byte limit",
                    read_buffer_size, MAX_READ_BUFFER_SIZE
                ),
            ));
        }

        Ok(())
    }

    fn reject_zero(name: &str, value: Option<usize>) -> Result<(), io::Error> {
        if value != Some(0) {
            return Ok(());
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Config::{} is 0", name),
        ))
    }

    /// Makes pending changes to the WAL directory itself durable.
    ///
    /// Synchronizing a chunk file only persists its contents. Until the
    /// directory is synchronized too, a newly created chunk file name or a
    /// removed one may not survive power loss.
    pub fn sync_dir(&self) -> Result<(), io::Error> {
        let directory = std::fs::File::open(&self.dir)?;
        directory.sync_all()
    }

    /// Returns the full path for a given chunk ID
    pub fn chunk_path(&self, chunk_id: ChunkId) -> String {
        let file_name = Self::chunk_file_name(chunk_id);
        format!("{}/{}", self.dir, file_name)
    }

    /// Generates the file name for a given chunk ID
    ///
    /// The file name format is "r-{padded_chunk_id}.wal"
    pub fn chunk_file_name(chunk_id: ChunkId) -> String {
        let file_name = num::format_pad_u64(*chunk_id);
        format!("r-{}.wal", file_name)
    }

    /// Parses a chunk file name and returns the chunk ID
    ///
    /// # Arguments
    /// * `file_name` - Name of the chunk file (format:
    ///   "r-{padded_chunk_id}.wal")
    ///
    /// # Returns
    /// * `Ok(u64)` - The chunk ID if parsing succeeds
    /// * `Err(InvalidChunkFileName)` - If the file name format is invalid
    pub fn parse_chunk_file_name(
        file_name: &str,
    ) -> Result<u64, InvalidChunkFileName> {
        // 1. Strip the ".wal" suffix or return an error if it's not there
        let without_suffix =
            file_name.strip_suffix(".wal").ok_or_else(|| {
                InvalidChunkFileName::new(file_name, "has no '.wal' suffix")
            })?;

        // 2. Strip the "r-" prefix or return an error if it's not there
        let without_prefix =
            without_suffix.strip_prefix("r-").ok_or_else(|| {
                InvalidChunkFileName::new(file_name, "has no 'r-' prefix")
            })?;

        if without_prefix.len() != 26 {
            return Err(InvalidChunkFileName::new(
                file_name,
                "does not have 26 digit after 'r-' prefix",
            ));
        }

        let digits = without_prefix
            .chars()
            .filter(|c| c.is_ascii_digit())
            .collect::<String>();

        // 3. Parse the remaining string as an u64
        digits.parse::<u64>().map_err(|e| {
            InvalidChunkFileName::new(
                file_name,
                format!("cannot parse as u64: {}", e),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::time::Duration;

    use super::Config;
    use super::MAX_READ_BUFFER_SIZE;
    use crate::ChunkId;

    #[test]
    fn test_config_defaults() {
        let config = Config::new("wal-dir");

        assert_eq!("wal-dir", config.dir);
        assert_eq!(64 * 1024 * 1024, config.read_buffer_size());
        assert_eq!(1024 * 1024, config.chunk_max_records());
        assert_eq!(1024 * 1024 * 1024, config.chunk_max_size());
        assert!(config.truncate_incomplete_record());
        assert_eq!(Duration::from_millis(1), config.flush_batch_wait());
        assert_eq!(2048, config.flush_batch_max_items());
        assert_eq!(64 * 1024 * 1024, config.flush_queue_max_bytes());
    }

    #[test]
    fn test_config_overrides() {
        let mut config = Config::new_full("wal-dir", Some(1), Some(2), Some(3));
        config.truncate_incomplete_record = Some(false);
        config.flush_batch_wait = Some(Duration::from_millis(9));
        config.flush_batch_max_items = Some(4);
        config.flush_queue_max_bytes = Some(5);

        assert_eq!(1, config.read_buffer_size());
        assert_eq!(2, config.chunk_max_records());
        assert_eq!(3, config.chunk_max_size());
        assert!(!config.truncate_incomplete_record());
        assert_eq!(Duration::from_millis(9), config.flush_batch_wait());
        assert_eq!(4, config.flush_batch_max_items());
        assert_eq!(5, config.flush_queue_max_bytes());
    }

    #[test]
    fn test_validate_accepts_defaults_and_overrides() -> Result<(), io::Error> {
        Config::new("wal-dir").validate()?;

        let mut config = Config::new_full("wal-dir", Some(1), Some(2), Some(3));
        config.flush_batch_max_items = Some(4);
        config.flush_queue_max_bytes = Some(5);
        config.validate()?;

        Ok(())
    }

    #[test]
    fn test_validate_rejects_empty_dir() {
        let err = Config::new("").validate().unwrap_err();

        assert_eq!(io::ErrorKind::InvalidInput, err.kind());
        assert_eq!("Config::dir is empty", err.to_string());
    }

    fn assert_zero_rejected(config: &Config, name: &str) {
        let err = config.validate().unwrap_err();

        assert_eq!(io::ErrorKind::InvalidInput, err.kind());
        assert_eq!(format!("Config::{name} is 0"), err.to_string());
    }

    #[test]
    fn test_validate_rejects_zero_sizes() {
        let mut config = Config::new("wal-dir");
        config.read_buffer_size = Some(0);
        assert_zero_rejected(&config, "read_buffer_size");

        let mut config = Config::new("wal-dir");
        config.chunk_max_records = Some(0);
        assert_zero_rejected(&config, "chunk_max_records");

        let mut config = Config::new("wal-dir");
        config.chunk_max_size = Some(0);
        assert_zero_rejected(&config, "chunk_max_size");

        let mut config = Config::new("wal-dir");
        config.flush_batch_max_items = Some(0);
        assert_zero_rejected(&config, "flush_batch_max_items");

        let mut config = Config::new("wal-dir");
        config.flush_queue_max_bytes = Some(0);
        assert_zero_rejected(&config, "flush_queue_max_bytes");
    }

    #[test]
    fn test_validate_rejects_oversized_read_buffer() {
        let mut config = Config::new("wal-dir");
        config.read_buffer_size = Some(MAX_READ_BUFFER_SIZE + 1);

        let err = config.validate().unwrap_err();

        assert_eq!(io::ErrorKind::InvalidInput, err.kind());
        assert_eq!(
            "Config::read_buffer_size 1073741825 exceeds the 1073741824 byte limit",
            err.to_string()
        );
    }

    #[test]
    fn test_sync_dir() -> Result<(), io::Error> {
        let td = tempfile::tempdir()?;
        let config = Config::new(td.path().to_str().unwrap());

        config.sync_dir()?;

        let missing_dir = format!("{}/absent", config.dir);
        let missing = Config::new(missing_dir);
        let err = missing.sync_dir().unwrap_err();
        assert_eq!(io::ErrorKind::NotFound, err.kind());

        Ok(())
    }

    #[test]
    fn test_chunk_path_and_file_name() {
        let config = Config::new("wal-dir");

        assert_eq!(
            "r-00_000_000_000_001_200_000.wal",
            Config::chunk_file_name(ChunkId(1_200_000))
        );
        assert_eq!(
            "wal-dir/r-00_000_000_000_001_200_000.wal",
            config.chunk_path(ChunkId(1_200_000))
        );
    }

    #[test]
    fn test_parse_chunk_file_name() {
        assert_eq!(
            Config::parse_chunk_file_name("r-10_100_000_000_001_200_000.wal"),
            Ok(10_100_000_000_001_200_000)
        );

        assert!(
            Config::parse_chunk_file_name("r-10_100_000_000_001_200_000_1.wal")
                .is_err()
        );
        assert!(Config::parse_chunk_file_name("r-1000000000.wal").is_err());
        assert!(
            Config::parse_chunk_file_name("r-10_100_000_000_001_200_000.wall")
                .is_err()
        );
        assert!(
            Config::parse_chunk_file_name("rrr-10_100_000_000_001_200_000.wal")
                .is_err()
        );

        let bad_file_name = format!("r-{}.wal", "_".repeat(26));
        let err = Config::parse_chunk_file_name(&bad_file_name).unwrap_err();
        assert_eq!(bad_file_name, err.bad_file_name);
        assert!(err.reason.contains("cannot parse as u64"));
    }
}
