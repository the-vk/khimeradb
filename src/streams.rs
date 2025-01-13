use std::{io::{Read, Seek, SeekFrom, Write}, path::PathBuf};

pub struct FileSegmentStream {
    root: PathBuf,
    segments: Vec<Segment>,
    position: u64,
    max_segment_size: u64,
}

impl FileSegmentStream {
    pub fn new(root: PathBuf, max_segment_size: u64) -> FileSegmentStream {
        if !root.is_dir() {
            panic!("Root path must be a directory");
        }

        FileSegmentStream {
            root,
            segments: Vec::new(),
            position: 0,
            max_segment_size,
        }
    }
}

impl Read for FileSegmentStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.segments.is_empty() {
            return Ok(0);
        }
        
        // Find segment using binary search
        let segment_index = match self.segments.binary_search_by(|segment| {
            if self.position < segment.start {
                std::cmp::Ordering::Greater
            } else if self.position >= segment.end {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        }) {
            Ok(index) => index,
            Err(_) => return Ok(0), // Position is outside of any segment
        };

        let mut total_read = 0;
        let mut current_segment = segment_index;

        while total_read < buf.len() && current_segment < self.segments.len() {
            let segment = &mut self.segments[current_segment];
            let offset = if current_segment == segment_index {
                self.position - segment.start
            } else {
                0
            };
            
            segment.file.seek(SeekFrom::Start(offset))?;
            let read = segment.file.read(&mut buf[total_read..])?;
            if read == 0 {
                if current_segment + 1 < self.segments.len() {
                    current_segment += 1;
                } else {
                    break;
                }
            }
            total_read += read;
        }

        Ok(total_read)
    }
}

impl Write for FileSegmentStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let size = buf.len() as u64;
        let current_pos = self.position;
        
        if self.segments.is_empty() || self.segments.last().map(|s| s.size()).unwrap() > self.max_segment_size {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(self.root.join(format!("{}.log", self.segments.len())))?;
            let segment = Segment::new(file, current_pos);
            self.segments.push(segment);
        }

        let segment = self.segments.last_mut().unwrap();
        segment.file.write_all(buf)?;
        segment.end = current_pos + size;
        self.position += size;
        Ok(size as usize)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if let Some(segment) = self.segments.last_mut() {
            segment.file.flush()
        } else {
            Ok(())
        }
    }
}

impl Seek for FileSegmentStream {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Start(position) => {
                self.position = position;
            },
            SeekFrom::End(position) => {
                let end_position = self.segments.iter().fold(0, |acc, segment| acc + segment.size()) as i64 + position;
                if end_position < 0 {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid seek to a negative position"));
                }
                self.position = end_position as u64;
            },
            SeekFrom::Current(position) => {
                let current_position = self.position as i64 + position;
                if current_position < 0 {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid seek to a negative position"));
                }
                self.position = current_position as u64;
            }
        }

        Ok(self.position)
    }
}

pub struct Segment {
    file: std::fs::File,
    start: u64,
    end: u64,
}

impl Segment {
    pub fn new(file: std::fs::File, start: u64) -> Segment {
        let end = start;
        Segment {
            file,
            start,
            end,
        }
    }

    pub fn size(&self) -> u64 {
        self.end - self.start
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_test_dir() -> TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn test_file_segment_stream_write() {
        let dir = setup_test_dir();
        let mut stream = FileSegmentStream::new(dir.path().to_path_buf(), 1024);
        
        let data = b"Hello, World!";
        assert_eq!(stream.write(data).unwrap(), data.len());
        assert_eq!(stream.position, data.len() as u64);
        assert_eq!(stream.segments.len(), 1);
        assert_eq!(stream.segments[0].size(), data.len() as u64);
    }

    #[test]
    fn test_file_segment_stream_multiple_segments() {
        let dir = setup_test_dir();
        let mut stream = FileSegmentStream::new(dir.path().to_path_buf(), 10);
        
        let data = b"Hello, World!";
        stream.write(data).unwrap();
        stream.write(data).unwrap();
        
        assert_eq!(stream.segments.len(), 2);
        assert!(stream.segments[0].size() == 13);
        assert!(stream.segments[1].size() == 13);
    }

    #[test]
    fn test_file_segment_stream_read() {
        let dir = setup_test_dir();
        let mut stream = FileSegmentStream::new(dir.path().to_path_buf(), 1024);
        
        let data = b"Hello, World!";
        stream.write(data).unwrap();
        
        stream.seek(SeekFrom::Start(0)).unwrap();
        let mut buf = vec![0; data.len()];
        assert_eq!(stream.read(&mut buf).unwrap(), data.len());
        assert_eq!(&buf, data);
    }

    #[test]
    fn test_file_segment_stream_seek() {
        let dir = setup_test_dir();
        let mut stream = FileSegmentStream::new(dir.path().to_path_buf(), 1024);
        
        let data = b"Hello, World!";
        stream.write(data).unwrap();
        
        stream.seek(SeekFrom::Start(7)).unwrap();
        let mut buf = vec![0; 6];
        stream.read(&mut buf).unwrap();
        assert_eq!(&buf, b"World!");
    }

    #[test]
    fn test_file_segment_stream_read_across_segments() {
        let dir = setup_test_dir();
        let mut stream = FileSegmentStream::new(dir.path().to_path_buf(), 10);
        
        stream.write(b"Hello, ").unwrap();
        
        stream.write(b"World!").unwrap();
        
        stream.seek(SeekFrom::Start(0)).unwrap();
        let mut buf = vec![0; 13];
        assert_eq!(stream.read(&mut buf).unwrap(), 13);
        assert_eq!(&buf, b"Hello, World!");
    }

    #[test]
    fn test_file_segment_stream_seek_negative() {
        let dir = setup_test_dir();
        let mut stream = FileSegmentStream::new(dir.path().to_path_buf(), 1024);
        
        let data = b"Hello, World!";
        stream.write(data).unwrap();
        
        // Should succeed - seeking from end
        assert!(stream.seek(SeekFrom::End(-5)).is_ok());
        
        // Should fail - seeking before start
        assert!(stream.seek(SeekFrom::End(-20)).is_err());
        assert!(stream.seek(SeekFrom::Current(-20)).is_err());
    }
}