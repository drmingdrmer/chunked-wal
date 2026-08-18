use std::io;
use std::sync::Arc;

use codeq::Encode;

use crate::ChunkId;
use crate::Config;
use crate::chunk::Chunk;
use crate::chunk::create_chunk_file;
use crate::chunk::file_slot::FileSlot;
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
    /// Constructs the in-memory state of a new chunk without creating its
    /// file.
    ///
    /// Returns the open chunk together with the encoded bytes of its leading
    /// record. The chunk's file slot is left pending; the caller is
    /// responsible for creating the file, writing the leading bytes at its
    /// start, and publishing the handle via the slot.
    pub(crate) fn prepare(
        chunk_id: ChunkId,
        initial_record: Rec,
    ) -> Result<(Self, Vec<u8>), io::Error> {
        let chunk = Chunk {
            f: FileSlot::pending(),
            global_offsets: vec![*chunk_id],
            truncated: None,
            _p: Default::default(),
        };

        let mut open = Self {
            pending_data: Vec::new(),
            chunk,
        };

        open.append_record(&initial_record)?;
        let leading_bytes = open.take_pending_data();

        Ok((open, leading_bytes))
    }

    /// Creates a new chunk file and its in-memory state synchronously.
    pub(crate) fn create(
        config: Arc<Config>,
        chunk_id: ChunkId,
        initial_record: Rec,
    ) -> Result<Self, io::Error> {
        let (open, leading_bytes) = Self::prepare(chunk_id, initial_record)?;

        let path = config.chunk_path(chunk_id);
        let f = create_chunk_file(&path, &leading_bytes)?;

        open.chunk.f.set(Arc::new(f));

        Ok(open)
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
