use std::fmt;
use std::fs::File;
use std::sync::Arc;

use crate::ChunkId;
use crate::WalTypes;
use crate::wal::file_persisted::ChunkPersistedCallback;

pub(crate) struct FileEntry<W>
where W: WalTypes
{
    pub(crate) starting_offset: u64,
    pub(crate) f: Arc<File>,

    /// Called after this file has been successfully synced.
    ///
    /// Receives the file, its starting offset, and the synced offset.
    /// The callback may be called multiple times.
    pub(crate) on_persisted: ChunkPersistedCallback<W>,
    /// for debug
    pub(crate) sync_id: u64,
}

impl<W> fmt::Display for FileEntry<W>
where W: WalTypes
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FileEntry{{ starting_offset: {}, sync_id: {} }}",
            ChunkId(self.starting_offset),
            self.sync_id
        )
    }
}

impl<W> fmt::Debug for FileEntry<W>
where W: WalTypes
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileEntry")
            .field("starting_offset", &ChunkId(self.starting_offset))
            .field("sync_id", &self.sync_id)
            .finish()
    }
}

impl<W> FileEntry<W>
where W: WalTypes
{
    pub(crate) fn new(
        starting_offset: u64,
        f: Arc<File>,
        on_persisted: ChunkPersistedCallback<W>,
    ) -> Self {
        Self {
            starting_offset,
            f,
            on_persisted,
            sync_id: 0,
        }
    }
}
