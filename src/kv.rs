use std::collections::BTreeMap;

const SEGMENT_SIZE_LIMIT: usize = 1024; // 1KB limit per segment

pub struct SSTable {
    segments: Vec<BTreeMap<String, Option<Vec<u8>>>>,
    data_size: Vec<usize>,
}

impl SSTable {
    pub fn new() -> Self {
        SSTable {
            segments: vec![BTreeMap::new()],
            data_size: vec![0],
        }
    }

    pub fn insert(&mut self, key: &str, value: &[u8]) {
        let key = key.to_owned();
        let key_len = key.len();
        let value_len = value.len();
        let last_index = self.segments.len() - 1;

        if let Some(Some(old_value)) = self.segments[last_index].get(&key) {
            self.data_size[last_index] -= old_value.len();
        } else {
            self.data_size[last_index] += key_len;
        }
        self.data_size[last_index] += value_len;
        self.segments[last_index].insert(key, Some(value.to_owned()));

        // Create new segment if needed
        if self.data_size[last_index] > SEGMENT_SIZE_LIMIT {
            self.segments.push(BTreeMap::new());
            self.data_size.push(0);
        }
    }

    pub fn get(&self, key: &str) -> Option<Box<[u8]>> {
        // Search segments from newest to oldest
        for segment in self.segments.iter().rev() {
            if let Some(value) = segment.get(key) {
                return value.as_ref().map(|v| v.clone().into_boxed_slice());
            }
        }
        None
    }

    pub fn delete(&mut self, key: &str) {
        let key = key.to_owned();
        let last_segment = self.segments.len() - 1;
        self.segments[last_segment].insert(key, None);
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
        assert_eq!(table.data_size[0], 0);
        
        table.insert("key1", b"value1");
        assert_eq!(table.data_size[0], 4 + 6); // "key1" + "value1" lengths
        
        table.insert("key1", b"new_value");
        assert_eq!(table.data_size[0], 4 + 9); // "key1" + "new_value" lengths
        
        table.insert("key2", b"value2");
        assert_eq!(table.data_size[0], (4 + 9) + (4 + 6)); // ("key1" + "new_value") + ("key2" + "value2") lengths
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
}