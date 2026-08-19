use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::sync::Arc;

use codeq::Encode;

use crate::ChunkId;
use crate::Config;
use crate::chunk::Chunk;
use crate::types::Segment;

#[derive(Debug)]
pub(crate) struct OpenChunk<Rec> {
    pending_data: Vec<u8>,
    pub(crate) chunk: Chunk<Rec>,
}

impl<Rec> OpenChunk<Rec> {
    /// Creates a new open chunk from an existing chunk.
    pub(crate) fn new(chunk: Chunk<Rec>) -> Self {
        Self {
            pending_data: Vec::new(),
            chunk,
        }
    }

    pub(crate) fn take_pending_data(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.pending_data)
    }
}

impl<Rec> OpenChunk<Rec>
where Rec: Encode
{
    pub(crate) fn create_empty(
        config: Arc<Config>,
        chunk_id: ChunkId,
    ) -> Result<Self, io::Error> {
        let f = Self::create_file(config.clone(), chunk_id)?;

        let open = Self::from_file(chunk_id, f)?;

        Ok(open)
    }

    pub(crate) fn from_file(
        chunk_id: ChunkId,
        f: Arc<File>,
    ) -> Result<Self, io::Error> {
        let record_offsets = vec![*chunk_id];

        let chunk = Chunk {
            f,
            global_offsets: record_offsets,
            truncated: None,
            _p: Default::default(),
        };

        let open = Self {
            pending_data: Vec::new(),
            chunk,
        };

        Ok(open)
    }

    /// Create a new open chunk and append an initial record to it.
    /// But does not fsync it.
    pub(crate) fn create_with_initial_record(
        config: Arc<Config>,
        chunk_id: ChunkId,
        initial_record: Rec,
    ) -> Result<Self, io::Error> {
        let mut open = Self::create_empty(config, chunk_id)?;

        open.append_record(&initial_record)?;
        open.chunk.f.write_all(&open.pending_data)?;
        open.pending_data.clear();

        Ok(open)
    }

    pub(crate) fn create_file(
        config: Arc<Config>,
        chunk_id: ChunkId,
    ) -> Result<Arc<File>, io::Error> {
        let path = config.chunk_path(chunk_id);
        let f = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(path)?;

        Ok(Arc::new(f))
    }

    pub(crate) fn append_record(
        &mut self,
        rec: &Rec,
    ) -> Result<Segment, io::Error> {
        let size = rec.encode(&mut self.pending_data)?;

        self.chunk.append_record_size(size as u64);

        Ok(self.chunk.last_segment())
    }
}
