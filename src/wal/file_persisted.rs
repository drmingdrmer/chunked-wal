use std::fmt;
use std::fs::File;
use std::sync::Arc;

use crate::ChunkId;
use crate::WalTypes;

pub struct ChunkPersisted {
    pub file: Arc<File>,
    pub starting_offset: u64,
    pub synced_offset: u64,
}

impl fmt::Debug for ChunkPersisted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChunkPersisted")
            .field("file", &self.file)
            .field("starting_offset", &ChunkId(self.starting_offset))
            .field("synced_offset", &ChunkId(self.synced_offset))
            .finish()
    }
}

/// Callback invoked after a chunk file is persisted.
///
/// The first argument describes the fsync event. The second argument carries
/// caller-defined state associated with the persisted chunk.
pub type ChunkPersistedFn<W> = Arc<
    dyn Fn(ChunkPersisted, Option<Arc<<W as WalTypes>::Checkpoint>>)
        + Send
        + Sync,
>;

pub(crate) struct ChunkPersistedCallback<W>
where W: WalTypes
{
    callback: ChunkPersistedFn<W>,
    prev_chunk_state: Option<Arc<W::Checkpoint>>,
}

impl<W> ChunkPersistedCallback<W>
where W: WalTypes
{
    pub(crate) fn new(
        callback: ChunkPersistedFn<W>,
        prev_chunk_state: Option<Arc<W::Checkpoint>>,
    ) -> Self {
        Self {
            callback,
            prev_chunk_state,
        }
    }
}

impl<W> ChunkPersistedCallback<W>
where W: WalTypes
{
    pub(crate) fn call(&self, persisted: ChunkPersisted) {
        (self.callback)(persisted, self.prev_chunk_state.clone());
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::mpsc::SyncSender;

    use crate::ChunkPersisted;
    use crate::WalTypes;
    use crate::wal::file_persisted::ChunkPersistedCallback;
    use crate::wal::file_persisted::ChunkPersistedFn;

    #[derive(Debug, Default, Clone, PartialEq, Eq)]
    struct TestWal;

    impl WalTypes for TestWal {
        type Action = String;
        type Checkpoint = String;
        type Callback = SyncSender<Result<(), io::Error>>;
    }

    #[test]
    fn test_chunk_persisted_debug() -> Result<(), io::Error> {
        let file = tempfile::tempfile()?;
        let persisted = ChunkPersisted {
            file: Arc::new(file),
            starting_offset: 12,
            synced_offset: 34,
        };

        let got = format!("{persisted:?}");

        assert!(got.contains("ChunkPersisted"));
        assert!(got.contains("starting_offset: ChunkId(12)"));
        assert!(got.contains("synced_offset: ChunkId(34)"));

        Ok(())
    }

    #[test]
    fn test_chunk_persisted_callback_keeps_state() -> Result<(), io::Error> {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let callback: ChunkPersistedFn<TestWal> = {
            let calls = calls.clone();
            Arc::new(move |persisted, checkpoint| {
                calls.lock().unwrap().push((
                    persisted.starting_offset,
                    persisted.synced_offset,
                    checkpoint.as_deref().cloned(),
                ));
            })
        };

        let cb = ChunkPersistedCallback::<TestWal>::new(
            callback,
            Some(Arc::new("prev".to_string())),
        );

        cb.call(ChunkPersisted {
            file: Arc::new(tempfile::tempfile()?),
            starting_offset: 1,
            synced_offset: 2,
        });
        cb.call(ChunkPersisted {
            file: Arc::new(tempfile::tempfile()?),
            starting_offset: 3,
            synced_offset: 4,
        });

        assert_eq!(
            vec![
                (1, 2, Some("prev".to_string())),
                (3, 4, Some("prev".to_string())),
            ],
            *calls.lock().unwrap()
        );

        Ok(())
    }
}
