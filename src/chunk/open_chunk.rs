use std::fs::File;
use std::io;
use std::sync::Arc;

use codeq::Encode;

use crate::ChunkId;
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

    pub(crate) fn from_created_file(
        file: Arc<File>,
        chunk_id: ChunkId,
        initial_record_size: u64,
    ) -> Self {
        let start = chunk_id.offset();
        let chunk = Chunk {
            f: file,
            global_offsets: vec![start, start + initial_record_size],
            truncated: None,
            _p: Default::default(),
        };
        Self::new(chunk)
    }
}

impl<Rec> OpenChunk<Rec>
where Rec: Encode
{
    pub(crate) fn encode_initial_record(
        initial_record: &Rec,
    ) -> Result<Vec<u8>, io::Error> {
        let mut data = Vec::new();
        initial_record.encode(&mut data)?;
        Ok(data)
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
