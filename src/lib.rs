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

impl<'a> IntoIterator for &'a MemoryLog {
    type Item = &'a [i8];
    type IntoIter = MemoryLogIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        MemoryLogIterator::new(&self.entries)
    }
}

pub struct MemoryLogIterator<'a> {
    // The log entries
    entries: &'a Vec<Vec<i8>>,
    // The current index
    index: usize
}

impl<'a> MemoryLogIterator<'a> {
    // Create a new MemoryLogIterator
    pub fn new(entries: &'a Vec<Vec<i8>>) -> MemoryLogIterator<'a> {
        MemoryLogIterator {
            entries: entries,
            index: 0
        }
    }
}

impl<'a> Iterator for MemoryLogIterator<'a> {
    type Item = &'a [i8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.entries.len() {
            let entry = &self.entries[self.index];
            self.index += 1;
            Some(entry)
        } else {
            None
        }
    }
}