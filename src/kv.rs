use std::collections::BTreeMap;
use std::io::{self, Write, Read};
use std::path::{Path, PathBuf};

const SEGMENT_SIZE_LIMIT: usize = 1024;

struct SSTableSegment {
    data: BTreeMap<String, Option<Vec<u8>>>,
    size: usize,
    serial: u64,
}

impl SSTableSegment {
    fn new(serial: u64) -> Self {
        SSTableSegment {
            data: BTreeMap::new(),
            size: 0,
            serial,
        }
    }

    fn insert(&mut self, key: String, value: Option<Vec<u8>>) {
        if let Some(Some(old_value)) = self.data.get(&key) {
            self.size -= old_value.len();
        } else {
            self.size += key.len();
        }
        if let Some(new_value) = &value {
            self.size += new_value.len();
        }
        self.data.insert(key, value);
        self.serial += 1;
    }

    fn delete(&mut self, key: String) {
        if let Some(Some(old_value)) = self.data.get(&key) {
            self.size -= old_value.len();
        }
        self.data.insert(key, None);

        self.serial += 1;
    }
}

pub struct SSTable {
    path: PathBuf,
    segments: Vec<SSTableSegment>,
}

impl SSTable {
    pub fn try_new(path: &Path) -> io::Result<Self> {
        let mut segments = SSTable::read(path).unwrap_or_default();
        if segments.is_empty() {
            segments.push(SSTableSegment::new(0));
        }
        Ok(SSTable {
            path: path.to_path_buf(),
            segments,
        })
    }

    pub fn insert(&mut self, key: &str, value: &[u8]) -> io::Result<()> {
        let key = key.to_owned();
        let last_index = self.segments.len() - 1;
        
        self.segments[last_index].insert(key, Some(value.to_vec()));

        if self.segments[last_index].size > SEGMENT_SIZE_LIMIT {
            self.add_segment()?;
        }
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<Box<[u8]>> {
        for segment in self.segments.iter().rev() {
            if let Some(value) = segment.data.get(key) {
                return value.as_ref().map(|v| v.clone().into_boxed_slice());
            }
        }
        None
    }

    pub fn delete(&mut self, key: &str) {
        let key = key.to_owned();
        let last_segment = self.segments.len() - 1;
        self.segments[last_segment].delete(key);
    }

    pub fn compact(&mut self) {
        let mut merged = BTreeMap::new();
        
        for segment in &self.segments {
            for (key, value) in &segment.data {
                merged.insert(key.clone(), value.clone());
            }
        }

        let last_serial = self.segments.last().unwrap().serial;

        let mut new_segments = vec![SSTableSegment::new(last_serial)];
        let mut current_segment = 0;

        for (key, value) in merged {
            let segment = &mut new_segments[current_segment];
            let entry_size = key.len() + value.as_ref().map_or(0, |v| v.len());

            segment.insert(key, value);

            if segment.size + entry_size > SEGMENT_SIZE_LIMIT {
                let segment_serial = segment.serial;
                new_segments.push(SSTableSegment::new(segment_serial));
                current_segment += 1;
            }
        }

        self.segments = new_segments;
    }

    fn add_segment(&mut self) -> io::Result<()> {
        let last_index = self.segments.len() - 1;
        self.segments.push(SSTableSegment::new(self.segments[last_index].serial));
        self.write(&self.path)?;
        Ok(())
    }

    fn read(path: &Path) -> io::Result<Vec<SSTableSegment>> {
        if !path.is_dir() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Path is not a directory"));
        }

        let mut segments = Vec::new();
        let mut serial = 0;

        // Helper function to parse segment serial from path
        fn parse_serial(path: &Path) -> Option<u64> {
            path.file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
        }

        // Collect and validate files
        let mut entries: Vec<_> = path.read_dir()?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_file())
            .collect();

        // Validate files before processing
        for path in &entries {
            if path.extension().and_then(|s| s.to_str()) != Some("sst") {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, 
                    format!("Invalid file extension: {:?}", path)));
            }
            if parse_serial(path).is_none() {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, 
                    format!("Invalid segment file name: {:?}", path)));
            }
        }

        // Sort by serial number
        entries.sort_by_key(|p| parse_serial(p).unwrap());

        // Process files in order
        for path in entries {
            let mut file = std::fs::File::open(&path)?;
            let file_serial = parse_serial(&path).unwrap();
            let segment = SSTable::read_segment(&mut file, serial)?;
            
            if file_serial != segment.serial {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid segment serial number"));
            }

            serial = segment.serial;
            segments.push(segment);
        }

        Ok(segments)
    }

    fn write(&self, path: &Path) -> io::Result<()> {
        if !path.is_dir() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("Path {:?} is not a directory", path)));
        }

        for s in &self.segments[..self.segments.len()-1] {
            let filename = format!("{}.sst", s.serial);
            let file_path = path.join(&filename);
            if file_path.exists() {
                continue;
            }
            let mut file = std::fs::File::create(file_path)?;
            SSTable::write_segment(&mut file, s)?;
        }

        Ok(())
    }

    fn write_segment<W: Write>(writer: &mut W, segment: &SSTableSegment) -> io::Result<()> {
        for (key, value) in &segment.data {
            // Write key as UTF-8 followed by null terminator
            writer.write_all(key.as_bytes())?;
            writer.write_all(&[0])?;

            match value {
                Some(v) => {
                    // Write value length as u32 (4 bytes)
                    writer.write_all(&(v.len() as u32).to_le_bytes())?;
                    // Write value bytes
                    writer.write_all(v)?;
                }
                None => {
                    // For deleted entries, write length as 0
                    writer.write_all(&[0, 0, 0, 0])?;
                }
            }
        }
        writer.flush()?;
        Ok(())
    }

    fn read_segment<R: Read>(reader: &mut R, initial_serial: u64) -> io::Result<SSTableSegment> {
        let mut segment = SSTableSegment::new(initial_serial);
        let mut buffer = Vec::new();
        
        loop {
            // Read key until null terminator
            buffer.clear();
            let mut byte = [0u8];
            
            loop {
                match reader.read_exact(&mut byte) {
                    Ok(_) if byte[0] == 0 => break,
                    Ok(_) => buffer.push(byte[0]),
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        return if buffer.is_empty() {
                            Ok(segment)
                        } else {
                            Err(e)
                        }
                    }
                    Err(e) => return Err(e),
                }
            }
            
            let key = String::from_utf8(buffer.clone())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            
            // Read value length
            let mut len_bytes = [0u8; 4];
            reader.read_exact(&mut len_bytes)?;
            let value_len = u32::from_le_bytes(len_bytes) as usize;
            
            if value_len == 0 {
                segment.insert(key, None);
            } else {
                // Read value
                if buffer.len() < value_len {
                    buffer.resize(value_len, 0);
                }
                reader.read_exact(&mut buffer[..value_len])?;
                segment.insert(key, Some(buffer[..value_len].to_vec()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use io::Cursor;
    use tempfile::tempdir;

    fn filler() -> Vec<u8> {
        vec![0u8; SEGMENT_SIZE_LIMIT]
    }

    #[test]
    fn test_insert_and_get() {
        let mut table = SSTable::try_new(tempdir().unwrap().path()).unwrap();
        table.insert("key1", b"value1").unwrap();
        assert_eq!(&*table.get("key1").unwrap(), b"value1");
    }

    #[test]
    fn test_overwrite_value() {
        let mut table = SSTable::try_new(tempdir().unwrap().path()).unwrap();
        table.insert("key1", b"value1").unwrap();
        table.insert("key1", b"value2").unwrap();
        assert_eq!(&*table.get("key1").unwrap(), b"value2");
    }

    #[test]
    fn test_get_non_existent() {
        let table = SSTable::try_new(tempdir().unwrap().path()).unwrap();
        assert!(table.get("missing").is_none());
    }

    #[test]
    fn test_empty_value() {
        let mut table = SSTable::try_new(tempdir().unwrap().path()).unwrap();
        table.insert("empty", b"").unwrap();
        assert_eq!(&*table.get("empty").unwrap(), b"");
    }

    #[test]
    fn test_multiple_entries() {
        let mut table = SSTable::try_new(tempdir().unwrap().path()).unwrap();
        let entries = vec![
            ("key1", b"value1"),
            ("key2", b"value2"),
            ("key3", b"value3"),
        ];

        for (k, v) in &entries {
            table.insert(k, *v).unwrap();
        }

        for (k, v) in &entries {
            assert_eq!(&*table.get(k).unwrap(), *v);
        }
    }

    #[test]
    fn test_data_size_tracking() {
        let mut table = SSTable::try_new(tempdir().unwrap().path()).unwrap();
        assert_eq!(table.segments[0].size, 0);
        
        table.insert("key1", b"value1").unwrap();
        assert_eq!(table.segments[0].size, 4 + 6); // "key1" + "value1" lengths
        
        table.insert("key1", b"new_value").unwrap();
        assert_eq!(table.segments[0].size, 4 + 9); // "key1" + "new_value" lengths
        
        table.insert("key2", b"value2").unwrap();
        assert_eq!(table.segments[0].size, (4 + 9) + (4 + 6)); // ("key1" + "new_value") + ("key2" + "value2") lengths
    }

    #[test]
    fn test_delete() {
        let mut table = SSTable::try_new(tempdir().unwrap().path()).unwrap();
        table.insert("key1", b"value1").unwrap();
        assert_eq!(&*table.get("key1").unwrap(), b"value1");
        
        table.delete("key1");
        assert!(table.get("key1").is_none());
    }

    #[test]
    fn test_delete_and_reinsert() {
        let mut table = SSTable::try_new(tempdir().unwrap().path()).unwrap();
        table.insert("key1", b"value1").unwrap();
        table.delete("key1");
        table.insert("key1", b"value2").unwrap();
        assert_eq!(&*table.get("key1").unwrap(), b"value2");
    }

    #[test]
    fn test_segment_chaining() {
        let dir = tempdir().unwrap();
        let mut table = SSTable::try_new(dir.path()).unwrap();
        
        // Fill first segment
        table.insert("key1", &filler()[..SEGMENT_SIZE_LIMIT/2]).unwrap();
        table.insert("key2", &filler()[..SEGMENT_SIZE_LIMIT/2]).unwrap();
        
        // This should create a new segment
        table.insert("key3", b"value3").unwrap();
        
        assert_eq!(table.segments.len(), 2);
        assert_eq!(&*table.get("key3").unwrap(), b"value3");
    }

    #[test]
    fn test_segment_value_shadowing() {
        let dir = tempdir().unwrap();
        let mut table = SSTable::try_new(dir.path()).unwrap();
        
        table.insert("key1", b"value1").unwrap();
        table.insert("filler", &filler()).unwrap();  // Force new segment
        table.insert("key1", b"value2").unwrap();
        
        assert_eq!(&*table.get("key1").unwrap(), b"value2");
    }

    #[test]
    fn test_delete_in_new_segment() {
        let dir = tempdir().unwrap();
        let mut table = SSTable::try_new(dir.path()).unwrap();
        
        table.insert("key1", b"value1").unwrap();
        table.insert("filler", &filler()).unwrap();  // Force new segment
        table.delete("key1");
        
        assert!(table.get("key1").is_none());
    }

    #[test]
    fn test_compact() {
        let dir = tempdir().unwrap();
        let mut table = SSTable::try_new(dir.path()).unwrap();
        
        table.insert("key1", b"value1").unwrap();
        table.insert("filler", &filler()).unwrap();
        
        table.insert("key1", b"value2").unwrap();
        table.insert("key2", b"value3").unwrap();
        
        assert_eq!(table.segments.len(), 2);
        table.compact();
        
        assert_eq!(&*table.get("key1").unwrap(), b"value2");
        assert_eq!(&*table.get("key2").unwrap(), b"value3");
        assert!(table.segments.len() >= 1);
    }

    #[test]
    fn test_compact_with_deletions() {
        let dir = tempdir().unwrap();
        let mut table = SSTable::try_new(dir.path()).unwrap();
        
        table.insert("key1", b"value1").unwrap();
        table.insert("key2", b"value2").unwrap();
        table.insert("filler", &filler()).unwrap();
        
        table.delete("key1");
        assert!(table.get("key1").is_none());
        table.compact();
        
        assert!(table.get("key1").is_none());
        assert_eq!(&*table.get("key2").unwrap(), b"value2");
    }

    #[test]
    fn test_write_segment() {
        let dir = tempdir().unwrap();
        let mut table = SSTable::try_new(dir.path()).unwrap();
        table.insert("key1", b"value1").unwrap();
        table.insert("key2", b"value2").unwrap();
        
        let mut cursor = Cursor::new(Vec::new());
        SSTable::write_segment(&mut cursor, &table.segments[0]).unwrap();
        
        let data = cursor.into_inner();
        
        // Verify that "key1" was written correctly
        let mut pos = 0;
        assert_eq!(&data[pos..pos+4], b"key1");  // key
        pos += 4;
        assert_eq!(data[pos], 0);                // null terminator
        pos += 1;
        assert_eq!(&data[pos..pos+4], &6u32.to_le_bytes());  // value length
        pos += 4;
        assert_eq!(&data[pos..pos+6], b"value1"); // value
        pos += 6;
        
        // Verify that "key2" was written correctly
        assert_eq!(&data[pos..pos+4], b"key2");  // key
        pos += 4;
        assert_eq!(data[pos], 0);                // null terminator
        pos += 1;
        assert_eq!(&data[pos..pos+4], &6u32.to_le_bytes());  // value length
        pos += 4;
        assert_eq!(&data[pos..pos+6], b"value2"); // value
        
        // Verify total length is correct
        assert_eq!(data.len(), 30);
    }

    #[test]
    fn test_read_segment() {
        let dir = tempdir().unwrap();
        let mut table = SSTable::try_new(dir.path()).unwrap();
        table.insert("key1", b"value1").unwrap();
        table.insert("key2", b"value2").unwrap();
        table.delete("key3");
        
        let mut buffer = Vec::new();
        {
            let mut cursor = Cursor::new(&mut buffer);
            SSTable::write_segment(&mut cursor, &table.segments[0]).unwrap();
        }
        
        let mut cursor = Cursor::new(&buffer);
        let segment = SSTable::read_segment(&mut cursor, 0).unwrap();
        
        // Verify segment contents
        assert_eq!(segment.data.len(), 3);
        assert_eq!(segment.data.get("key1").unwrap().as_ref().unwrap(), b"value1");
        assert_eq!(segment.data.get("key2").unwrap().as_ref().unwrap(), b"value2");
        assert!(segment.data.get("key3").unwrap().is_none());
        
        // Verify segment size tracking
        assert_eq!(segment.size, "key1".len() + "value1".len() + 
                               "key2".len() + "value2".len() +
                               "key3".len());
    }

    #[test]
    fn test_read_segment_empty() {
        let mut cursor = Cursor::new(Vec::new());
        let segment = SSTable::read_segment(&mut cursor, 0).unwrap();
        assert_eq!(segment.data.len(), 0);
        assert_eq!(segment.size, 0);
    }

    #[test]
    fn test_read_segment_invalid_utf8() {
        let invalid_data = vec![0xFF, 0xFF, 0x00];  // Invalid UTF-8 sequence
        let mut cursor = Cursor::new(&invalid_data);
        assert!(SSTable::read_segment(&mut cursor, 0).is_err());
    }

    #[test]
    fn test_write_read_table() {
        let dir = tempdir().unwrap();
        let mut table = SSTable::try_new(dir.path()).unwrap();
        table.insert("key1", b"value1").unwrap();
        table.insert("key2", b"value2").unwrap();
        table.insert("filler", &filler()).unwrap();
        table.insert("key3", b"value3").unwrap();
        table.delete("key2");
        
        table.write(dir.path()).unwrap();

        // Verify file names match segment serials
        let mut files: Vec<_> = fs::read_dir(dir.path()).unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();

        // Parse and sort by numeric serial
        files.sort_by_key(|name| {
            name.strip_suffix(".sst")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap()
        });
        
        assert_eq!(files.len(), table.segments.len() - 1); // last segment not written
        for (i, segment) in table.segments[..table.segments.len()-1].iter().enumerate() {
            assert_eq!(files[i], format!("{}.sst", segment.serial));
        }
        
        // Read table back
        let read_table = SSTable::try_new(dir.path()).unwrap();
        
        // Verify contents
        assert_eq!(read_table.segments.len(), table.segments.len() - 1);
        assert_eq!(&*read_table.get("key1").unwrap(), b"value1");
    }

    #[test]
    fn test_write_read_empty_table() {
        let dir = tempdir().unwrap();
        let table = SSTable::try_new(dir.path()).unwrap();
        
        table.write(dir.path()).unwrap();
        let read_table = SSTable::try_new(dir.path()).unwrap();
        
        assert_eq!(read_table.segments.len(), 1);
    }

    #[test]
    fn test_write_invalid_path() {
        let dir = tempdir().unwrap();
        let table = SSTable::try_new(dir.path()).unwrap();
        let result = table.write(Path::new("/nonexistent/path"));
        assert!(result.is_err());
    }

    #[test]
    fn test_read_invalid_path() {
        let result = SSTable::read(Path::new("/nonexistent/path"));
        assert!(result.is_err());
    }

    #[test]
    fn test_read_corrupted_file() {
        let dir = tempdir().unwrap();
        
        // Create corrupted segment file
        fs::write(
            dir.path().join("0.sst"),
            &[0xFF, 0xFF, 0xFF] // Invalid data
        ).unwrap();
        
        let result = SSTable::read(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_write_idempotency() {
        let dir = tempdir().unwrap();
        
        let mut table = SSTable::try_new(dir.path()).unwrap();
        table.insert("key1", b"value1").unwrap();
        table.insert("filler", &filler()).unwrap();
        
        // Write twice
        table.write(dir.path()).unwrap();
        table.write(dir.path()).unwrap();
        
        // Verify only one file exists
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn test_segment_overflow_writes_file() {
        let dir = tempdir().unwrap();
        let mut table = SSTable::try_new(dir.path()).unwrap();
        
        // No files initially
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
        
        // Fill first segment
        table.insert("key1", &filler()).unwrap();
        
        // Should create a new segment and write the first one
        table.insert("key2", b"value2").unwrap();
        
        // Verify file was written
        let files: Vec<_> = fs::read_dir(dir.path()).unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], format!("{}.sst", table.segments[0].serial));
        
        // Verify file contains the first segment's data
        let read_table = SSTable::try_new(dir.path()).unwrap();
        assert_eq!(read_table.segments.len(), 1);
        assert!(read_table.get("key1").is_some());
    }
}