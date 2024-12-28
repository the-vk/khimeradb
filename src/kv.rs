use std::collections::BTreeMap;

pub struct SSTable {
    segment: BTreeMap<String, Option<Vec<u8>>>,
    data_size: usize,
}

impl SSTable {
    pub fn new() -> Self {
        SSTable {
            segment: BTreeMap::new(),
            data_size: 0,
        }
    }

    pub fn insert(&mut self, key: &str, value: &[u8]) {
        let key = key.to_owned();
        // Subtract sizes of old entry if key exists
        if let Some(Some(old_value)) = self.segment.get(&key) {
            self.data_size -= key.len() + old_value.len();
        }
        // Add sizes of new entry
        self.data_size += key.len() + value.len();
        self.segment.insert(key, Some(value.to_owned()));
    }

    pub fn get(&self, key: &str) -> Option<Box<[u8]>> {
        self.segment.get(key)
            .and_then(|v| v.as_ref())
            .map(|v| v.clone().into_boxed_slice())
    }

    pub fn delete(&mut self, key: &str) {
        let key = key.to_owned();
        // Subtract sizes of old entry if key exists
        if let Some(Some(old_value)) = self.segment.get(&key) {
            self.data_size -= key.len() + old_value.len();
            self.segment.insert(key, None);
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
        assert_eq!(table.data_size, 0);
        
        table.insert("key1", b"value1");
        assert_eq!(table.data_size, 4 + 6); // "key1" + "value1" lengths
        
        table.insert("key1", b"new_value");
        assert_eq!(table.data_size, 4 + 9); // "key1" + "new_value" lengths
        
        table.insert("key2", b"value2");
        assert_eq!(table.data_size, (4 + 9) + (4 + 6)); // ("key1" + "new_value") + ("key2" + "value2") lengths
    }

    #[test]
    fn test_delete() {
        let mut table = SSTable::new();
        table.insert("key1", b"value1");
        assert_eq!(&*table.get("key1").unwrap(), b"value1");
        
        table.delete("key1");
        assert!(table.get("key1").is_none());
        assert_eq!(table.data_size, 0);
    }

    #[test]
    fn test_delete_and_reinsert() {
        let mut table = SSTable::new();
        table.insert("key1", b"value1");
        table.delete("key1");
        table.insert("key1", b"value2");
        assert_eq!(&*table.get("key1").unwrap(), b"value2");
    }
}