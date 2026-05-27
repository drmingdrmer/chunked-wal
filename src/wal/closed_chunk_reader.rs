use std::collections::BTreeMap;
use std::io;

use crate::ChunkId;
use crate::WALRecord;
use crate::WalTypes;
use crate::chunk::closed_chunk::ClosedChunk;
use crate::types::Segment;

#[derive(Debug, Clone)]
pub struct ClosedChunkReader<W>
where W: WalTypes
{
    chunks: BTreeMap<ChunkId, ClosedChunk<W>>,
}

impl<W> ClosedChunkReader<W>
where W: WalTypes
{
    pub(crate) fn new(chunks: BTreeMap<ChunkId, ClosedChunk<W>>) -> Self {
        Self { chunks }
    }

    pub fn read_record(
        &self,
        chunk_id: ChunkId,
        segment: Segment,
    ) -> Result<WALRecord<W>, io::Error> {
        let closed = self.chunks.get(&chunk_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Chunk not found: {}", chunk_id),
            )
        })?;

        closed.chunk.read_record(segment)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io;
    use std::os::unix::fs::FileExt;
    use std::sync::Arc;
    use std::sync::mpsc::SyncSender;

    use codeq::Decode;
    use codeq::Encode;

    use crate::Chunk;
    use crate::ChunkId;
    use crate::Config;
    use crate::WALRecord;
    use crate::WalTypes;
    use crate::chunk::closed_chunk::ClosedChunk;
    use crate::chunk::open_chunk::OpenChunk;
    use crate::types::Segment;
    use crate::wal::closed_chunk_reader::ClosedChunkReader;

    const TEST_ACTION_TYPE: u32 = 1;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestAction(String);

    impl Encode for TestAction {
        fn encode<W: io::Write>(&self, mut w: W) -> Result<usize, io::Error> {
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

    fn action(v: &str) -> WALRecord<TestWal> {
        WALRecord::Action(TestAction(v.to_string()))
    }

    fn build_reader() -> Result<(ClosedChunkReader<TestWal>, Segment), io::Error>
    {
        let td = tempfile::tempdir()?;
        let config = Config::new(td.path().to_str().unwrap());
        let config = Arc::new(config);
        let chunk_id = ChunkId(0);

        let mut open = OpenChunk::<WALRecord<TestWal>>::create(
            config.clone(),
            chunk_id,
            WALRecord::Checkpoint(String::new()),
        )?;
        open.append_record(&action("val"))?;
        let data = open.take_pending_data();
        let offset = open.chunk.f.metadata()?.len();
        open.chunk.f.write_all_at(&data, offset)?;

        let (chunk, records) = Chunk::<WALRecord<TestWal>>::open_with_truncate(
            config, chunk_id, true,
        )?;
        assert_eq!(
            vec![WALRecord::Checkpoint(String::new()), action("val"),],
            records
        );

        let segment = chunk.record_segment(1);

        let chunks = BTreeMap::from([(
            chunk_id,
            ClosedChunk::new(chunk, Arc::new("val".to_string())),
        )]);

        Ok((ClosedChunkReader::new(chunks), segment))
    }

    #[test]
    fn test_read_record() -> Result<(), io::Error> {
        let (reader, segment) = build_reader()?;

        assert_eq!(action("val"), reader.read_record(ChunkId(0), segment)?);

        Ok(())
    }

    #[test]
    fn test_read_record_returns_not_found_for_missing_chunk()
    -> Result<(), io::Error> {
        let (reader, segment) = build_reader()?;

        let err = reader.read_record(ChunkId(1), segment).unwrap_err();

        assert_eq!(io::ErrorKind::NotFound, err.kind());
        assert_eq!(
            "Chunk not found: ChunkId(00_000_000_000_000_000_001)",
            err.to_string()
        );

        Ok(())
    }
}
