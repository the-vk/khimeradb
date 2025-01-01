use std::collections::BTreeMap;
use std::io::{self, Write, Read, Cursor};

const SEGMENT_SIZE_LIMIT: usize = 1024;

struct SSTableSegment {
    data: BTreeMap<String, Option<Vec<u8>>>,
    size: usize,
}

impl SSTableSegment {
    fn new() -> Self {
        SSTableSegment {
            data: BTreeMap::new(),
            size: 0,
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
    }

    fn delete(&mut self, key: String) {
        if let Some(Some(old_value)) = self.data.get(&key) {
            self.size -= old_value.len();
        }
        self.data.insert(key, None);
    }
}

pub struct SSTable {
    segments: Vec<SSTableSegment>,
}

impl SSTable {
    pub fn new() -> Self {
        SSTable {
            segments: vec![SSTableSegment::new()],
        }
    }

    pub fn insert(&mut self, key: &str, value: &[u8]) {
        let key = key.to_owned();
        let last_index = self.segments.len() - 1;
        
        self.segments[last_index].insert(key, Some(value.to_vec()));

        if self.segments[last_index].size > SEGMENT_SIZE_LIMIT {
            self.segments.push(SSTableSegment::new());
        }
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

        let mut new_segments = vec![SSTableSegment::new()];
        let mut current_segment = 0;

        for (key, value) in merged {
            let segment = &mut new_segments[current_segment];
            let entry_size = key.len() + value.as_ref().map_or(0, |v| v.len());

            segment.insert(key, value);

            if segment.size + entry_size > SEGMENT_SIZE_LIMIT {
                new_segments.push(SSTableSegment::new());
                current_segment += 1;
            }
        }

        self.segments = new_segments;
    }

    fn write_segment<W: Write>(&self, writer: &mut W, segment: &SSTableSegment) -> io::Result<()> {
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
        Ok(())
    }

    fn read_segment<R: Read>(&self, reader: &mut R) -> io::Result<SSTableSegment> {
        let mut segment = SSTableSegment::new();
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

    #[test]
    fn test_insert_and_get() {
        let mut table = SSTable::new();
        table.insert("key1", b"value1");
        assert_eq!(&*table.get("key1").unwrap(), b"value1");
    }

    #[test]
    fn test_overwrite_value() {
        let mut table = SSTable::new();
        table.insert("key1", b"value1");
        table.insert("key1", b"value2");
        assert_eq!(&*table.get("key1").unwrap(), b"value2");
    }

    #[test]
    fn test_get_non_existent() {
        let table = SSTable::new();
        assert!(table.get("missing").is_none());
    }

    #[test]
    fn test_empty_value() {
        let mut table = SSTable::new();
        table.insert("empty", b"");
        assert_eq!(&*table.get("empty").unwrap(), b"");
    }

    #[test]
    fn test_multiple_entries() {
        let mut table = SSTable::new();
        let entries = vec![
            ("key1", b"value1"),
            ("key2", b"value2"),
            ("key3", b"value3"),
        ];

        for (k, v) in &entries {
            table.insert(k, *v);
        }

        for (k, v) in &entries {
            assert_eq!(&*table.get(k).unwrap(), *v);
        }
    }

    #[test]
    fn test_data_size_tracking() {
        let mut table = SSTable::new();
        assert_eq!(table.segments[0].size, 0);
        
        table.insert("key1", b"value1");
        assert_eq!(table.segments[0].size, 4 + 6); // "key1" + "value1" lengths
        
        table.insert("key1", b"new_value");
        assert_eq!(table.segments[0].size, 4 + 9); // "key1" + "new_value" lengths
        
        table.insert("key2", b"value2");
        assert_eq!(table.segments[0].size, (4 + 9) + (4 + 6)); // ("key1" + "new_value") + ("key2" + "value2") lengths
    }

    #[test]
    fn test_delete() {
        let mut table = SSTable::new();
        table.insert("key1", b"value1");
        assert_eq!(&*table.get("key1").unwrap(), b"value1");
        
        table.delete("key1");
        assert!(table.get("key1").is_none());
    }

    #[test]
    fn test_delete_and_reinsert() {
        let mut table = SSTable::new();
        table.insert("key1", b"value1");
        table.delete("key1");
        table.insert("key1", b"value2");
        assert_eq!(&*table.get("key1").unwrap(), b"value2");
    }

    #[test]
    fn test_segment_chaining() {
        let mut table = SSTable::new();
        
        // Fill first segment
        let large_value = vec![0u8; SEGMENT_SIZE_LIMIT/2];
        table.insert("key1", &large_value);
        table.insert("key2", &large_value);
        
        // This should create a new segment
        table.insert("key3", b"value3");
        
        assert_eq!(table.segments.len(), 2);
        assert_eq!(&*table.get("key3").unwrap(), b"value3");
    }

    #[test]
    fn test_segment_value_shadowing() {
        let mut table = SSTable::new();
        
        table.insert("key1", b"value1");
        // Force new segment
        let large_value = vec![0u8; SEGMENT_SIZE_LIMIT];
        table.insert("filler", &large_value);
        // Write to new segment
        table.insert("key1", b"value2");
        
        // Should return newest value
        assert_eq!(&*table.get("key1").unwrap(), b"value2");
    }

    #[test]
    fn test_delete_in_new_segment() {
        let mut table = SSTable::new();
        
        table.insert("key1", b"value1");
        // Force new segment
        let large_value = vec![0u8; SEGMENT_SIZE_LIMIT];
        table.insert("filler", &large_value);
        // Delete in new segment
        table.delete("key1");
        
        // Should return None even though old value exists in earlier segment
        assert!(table.get("key1").is_none());
    }

    #[test]
    fn test_compact() {
        let mut table = SSTable::new();
        
        // Fill first segment
        table.insert("key1", b"value1");
        let large_value = vec![0u8; SEGMENT_SIZE_LIMIT];
        table.insert("filler", &large_value);
        
        // Add to new segment
        table.insert("key1", b"value2");
        table.insert("key2", b"value3");
        
        assert_eq!(table.segments.len(), 2);
        table.compact();
        
        // Should maintain latest values and create minimum required segments
        assert_eq!(&*table.get("key1").unwrap(), b"value2");
        assert_eq!(&*table.get("key2").unwrap(), b"value3");
        assert!(table.segments.len() >= 1);
    }

    #[test]
    fn test_compact_with_deletions() {
        let mut table = SSTable::new();
        
        table.insert("key1", b"value1");
        table.insert("key2", b"value2");
        let large_value = vec![0u8; SEGMENT_SIZE_LIMIT];
        table.insert("filler", &large_value);
        
        table.delete("key1");
        assert!(table.get("key1").is_none());
        table.compact();
        
        assert!(table.get("key1").is_none());
        assert_eq!(&*table.get("key2").unwrap(), b"value2");
    }

    #[test]
    fn test_write_segment() {
        let mut table = SSTable::new();
        table.insert("key1", b"value1");
        table.insert("key2", b"value2");
        
        let mut cursor = Cursor::new(Vec::new());
        table.write_segment(&mut cursor, &table.segments[0]).unwrap();
        
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
        let mut table = SSTable::new();
        table.insert("key1", b"value1");
        table.insert("key2", b"value2");
        table.delete("key3");
        
        let mut buffer = Vec::new();
        {
            let mut cursor = Cursor::new(&mut buffer);
            table.write_segment(&mut cursor, &table.segments[0]).unwrap();
        }
        
        let mut cursor = Cursor::new(&buffer);
        let segment = table.read_segment(&mut cursor).unwrap();
        
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
        let table = SSTable::new();
        let mut cursor = Cursor::new(Vec::new());
        let segment = table.read_segment(&mut cursor).unwrap();
        assert_eq!(segment.data.len(), 0);
        assert_eq!(segment.size, 0);
    }

    #[test]
    fn test_read_segment_invalid_utf8() {
        let table = SSTable::new();
        let invalid_data = vec![0xFF, 0xFF, 0x00];  // Invalid UTF-8 sequence
        let mut cursor = Cursor::new(&invalid_data);
        assert!(table.read_segment(&mut cursor).is_err());
    }
}