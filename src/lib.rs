pub mod streams;

use std::{cell::RefCell, io::{Read, Seek, SeekFrom, Write}};

pub struct Log<T>
    where T: Read + Write + Seek {
    // The log entries
    storage: RefCell<T>,
}

impl <T> Log<T>
    where T: Read + Write + Seek {
    // Create a new MemoryLog
    pub fn new(storage: RefCell<T>) -> Log<T> {
        Log {
            storage
        }
    }

    // Append a new entry to the log
    pub fn append(&mut self, entry: &[u8]) -> std::io::Result<()> {
        let size = entry.len() as u32;
        let size_bytes = size.to_be_bytes();
        self.storage.borrow_mut().seek(SeekFrom::End(0))?;
        self.storage.borrow_mut().write(&size_bytes)?;
        self.storage.borrow_mut().write(entry)?;

        Ok(())
    }
}

impl<'a, T> IntoIterator for &'a Log<T>
    where T: Read + Write + Seek {
    type Item = Box<[u8]>;
    type IntoIter = LogIterator<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        LogIterator {
            log: &self.storage,
            position: 0,
        }
    }
}

pub struct LogIterator<'a, T>
    where T: Read + Write + Seek {
    log: &'a RefCell<T>,
    position: u64,
}

impl<'a, T> Iterator for LogIterator<'a, T>
    where T: Read + Write + Seek {
    type Item = Box<[u8]>;
    
    fn next(&mut self) -> Option<Self::Item> {
        let mut log = self.log.borrow_mut();
        if let Err(_) = log.seek(SeekFrom::Start(self.position)) {
            return None;
        }

        let mut size_bytes = [0; 4];
        match log.read(&mut size_bytes) {
            Ok(0) => return None,
            Err(_) => return None,
            _ => {}
        }

        let size = u32::from_be_bytes(size_bytes) as usize;
        let mut entry = vec![0; size];
        match log.read(&mut entry) {
            Ok(0) => return None,
            Err(_) => return None,
            _ => {}
        }

        self.position += 4 + size as u64;
        Some(entry.into_boxed_slice())
    }
}

#[cfg(test)]
mod tests {
    use crate::Log;
    use std::cell::RefCell;

    #[test]
    fn test_log() {
        let count = 100;
        let storage:Vec<u8> = Vec::new();
        let cursor = RefCell::new(std::io::Cursor::new(storage));
        let mut log = Log::new(cursor);
        let entry = [0; 100];
        for _ in 0..count {
            log.append(&entry).unwrap();
        }
        let mut count = 0;
        for _ in log.into_iter() {
            count += 1;
        }
        assert_eq!(count, count);
    }
}