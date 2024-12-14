use criterion::{black_box, criterion_group, criterion_main, Criterion};

use khimeradb::MemoryLog;

pub fn bench_memory_log_10000_appends(c: &mut Criterion) {
    let mut log = MemoryLog::new();
    let entry = [0; 100];
    
    c.bench_function("MemoryLog 10000 appends", |b| b.iter(|| {
        for _ in 0..black_box(10000) {
            log.append(&entry);
        }
    }));
}

criterion_group!(benches, bench_memory_log_10000_appends);
criterion_main!(benches);
