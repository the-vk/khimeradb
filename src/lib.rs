use std::{cell::RefCell, io, path::Path};

pub mod kv;
pub mod log;
pub mod streams;

pub struct SSTEngine {
    kv: kv::SSTable,
    log: log::Log<streams::FileSegmentStream>,
}

#[derive(Debug)]
enum LogOperation {
    Insert(String, Vec<u8>),
    Delete(String),
}

impl SSTEngine {
    pub fn try_new(path: &Path) -> io::Result<Self> {
        let kv = kv::SSTable::try_new(path.join("data").as_path())?;
        let file_segment_stream = streams::FileSegmentStream::new(path.join("log"), 1024*1024);
        let log = log::Log::new(RefCell::new(file_segment_stream));
        Ok(SSTEngine { kv, log })
    }

    pub fn get(&self, key: &str) -> io::Result<Option<Box<[u8]>>> {
        Ok(self.kv.get(key))
    }

    pub fn insert(&mut self, key: &str, value: &[u8]) -> io::Result<()> {
        self.write_log(LogOperation::Insert(key.to_string(), value.to_vec()))?;
        self.kv.insert(key, value)
    }

    fn write_log(&mut self, op: LogOperation) -> io::Result<()> {
        match op {
            LogOperation::Insert(key, value) => {
                let key_bytes = key.as_bytes();
                let mut entry = Vec::with_capacity(3 + key_bytes.len() + value.len());
                entry.push(1u8);
                entry.extend_from_slice(key.as_bytes());
                entry.push(0u8);
                entry.extend_from_slice(&value);
                entry.push(0u8);
                self.log.append(&entry)?;
            }
            LogOperation::Delete(key) => {
                let key_bytes = key.as_bytes();
                let mut entry = Vec::with_capacity(2 + key_bytes.len());
                entry.push(2u8);
                entry.extend_from_slice(key_bytes);
                entry.push(0u8);
                self.log.append(&entry)?;
            }
        }

        self.log.flush()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::fs;

    #[test]
    fn test_engine_creates_directories() {
        let root = tempdir().unwrap();
        let _engine = SSTEngine::try_new(root.path()).unwrap();
        
        let data_dir = root.path().join("data");
        let log_dir = root.path().join("log");
        
        assert!(data_dir.is_dir());
        assert!(log_dir.is_dir());
    }

    #[test]
    fn test_engine_insert() {
        let root = tempdir().unwrap();
        let mut engine = SSTEngine::try_new(root.path()).unwrap();

        engine.insert("key1", b"value1").unwrap();
        
        // Verify data exists
        assert_eq!(&*engine.get("key1").unwrap().unwrap(), b"value1");
        
        // Verify log file was created
        let log_files: Vec<_> = fs::read_dir(root.path().join("log")).unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(log_files.len(), 1);
    }

    #[test]
    fn test_engine_segment_overflow() {
        let root = tempdir().unwrap();
        let mut engine = SSTEngine::try_new(root.path()).unwrap();

        // Create enough data to force segment overflow
        let large_value = vec![0u8; 1024];
        engine.insert("key1", &large_value).unwrap();
        engine.insert("key2", b"value2").unwrap();

        // Verify data directory contains segment file
        let data_files: Vec<_> = fs::read_dir(root.path().join("data")).unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        
        assert!(!data_files.is_empty());
        assert!(data_files.iter().any(|f| f.ends_with(".sst")));

        // Verify log directory reflects the changes
        let log_files: Vec<_> = fs::read_dir(root.path().join("log")).unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(log_files.len() >= 1); // Should have at least one log file
        
        // Verify data is still accessible
        assert_eq!(&*engine.get("key2").unwrap().unwrap(), b"value2");
    }
}