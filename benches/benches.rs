use std::cell::RefCell;

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use khimeradb::Log;

pub fn bench_memory_log_10000_appends(c: &mut Criterion) {
    c.bench_function("MemoryLog 10000 appends", |b| b.iter(|| {
        let storage:Vec<u8> = Vec::new();
        let cursor = RefCell::new(std::io::Cursor::new(storage));
        let mut log = Log::new(cursor);
        let entry = [0; 100];

        for _ in 0..black_box(10000) {
            log.append(&entry);
        }
    }));
}

pub fn bench_memory_log_10000_iterator(c: &mut Criterion) {
    
    
    c.bench_function("MemoryLog 10000 iterator", |b| b.iter(|| {
        let storage:Vec<u8> = Vec::new();
        let cursor = RefCell::new(std::io::Cursor::new(storage));
        let mut log = Log::new(cursor);
        let entry = [0; 100];
        for _ in 0..10000 {
            log.append(&entry);
        }
        for _ in log.into_iter() {
        }
    }));
}

criterion_group!(benches,
    bench_memory_log_10000_appends,
    bench_memory_log_10000_iterator,
);
criterion_main!(benches);
