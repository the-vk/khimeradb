pub struct MemoryLog {
    // The log entries
    entries: Vec<Vec<i8>>
}

impl MemoryLog {
    // Create a new MemoryLog
    pub fn new() -> MemoryLog {
        MemoryLog {
            entries: Vec::new()
        }
    }

    // Append a new entry to the log
    pub fn append(&mut self, entry: &[i8]) {
        self.entries.push(entry.to_vec());
    }
}
