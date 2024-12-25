use std::cell::RefCell;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use khimeradb::{FileSegmentStream, Log};
use tempfile::tempfile;

pub fn bench_memory_log_10000_appends(c: &mut Criterion) {
    c.bench_function("MemoryLog 10000 appends", |b| b.iter(|| {
        let storage:Vec<u8> = Vec::new();
        let cursor = RefCell::new(std::io::Cursor::new(storage));
        let mut log = Log::new(cursor);
        let entry = [0; 100];

        for _ in 0..black_box(100000) {
            let _ = log.append(&entry);
        }
    }));
}

pub fn bench_memory_log_10000_iterator(c: &mut Criterion) {
    c.bench_function("MemoryLog 10000 iterator", |b| b.iter(|| {
        let storage:Vec<u8> = Vec::new();
        let cursor = RefCell::new(std::io::Cursor::new(storage));
        let mut log = Log::new(cursor);
        let entry = [0; 100];
        for _ in 0..100000 {
            let _ = log.append(&entry);
        }
        for _ in log.into_iter() {
        }
    }));
}

pub fn bench_file_log_10000_iterator(c: &mut Criterion) {
    c.bench_function("File Log 10000 iterator", |b| b.iter(|| {
        let file = tempfile().unwrap();
        let file = RefCell::new(file);
        let mut log = Log::new(file);
        let entry = [0; 100];
        for _ in 0..10000 {
            let _ = log.append(&entry);
        }
        for _ in log.into_iter() {
        }
    }));
}

pub fn bench_file_segment_log_10000_appends(c: &mut Criterion) {
    c.bench_function("FileSegmentLog 10000 appends", |b| b.iter(|| {
        let tempdir = tempfile::tempdir().unwrap();
        let storage = FileSegmentStream::new(tempdir.path().to_path_buf(), 1024);
        let mut log = Log::new(RefCell::new(storage));

        let data = [0; 1024];

        for _ in 0..black_box(10000) {
            let _ = log.append(&data);
        }
    }));
}

pub fn bench_file_segment_log_10000_iterator(c: &mut Criterion) {
    c.bench_function("FileSegmentLog 10000 iterations", |b| b.iter(|| {
        let tempdir = tempfile::tempdir().unwrap();
        let storage = FileSegmentStream::new(tempdir.path().to_path_buf(), 1024);
        let mut log = Log::new(RefCell::new(storage));

        let data = [0; 1024];

        for _ in 0..black_box(10000) {
            let _ = log.append(&data);
        }

        for _ in log.into_iter() {
        }
    }));
}

criterion_group!(benches,
    bench_memory_log_10000_appends,
    bench_memory_log_10000_iterator,
    bench_file_log_10000_iterator,
    bench_file_segment_log_10000_appends,
    bench_file_segment_log_10000_iterator
);
criterion_main!(benches);
