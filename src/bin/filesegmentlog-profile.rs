use std::cell::RefCell;

use khimeradb::{streams::FileSegmentStream, Log};
use tempfile::tempdir;

const MESSAGE_SIZE: usize = 1024;
const ITERATIONS: usize = 10000;

fn main() {
    println!("Running FileSegmentLog profile");

    println!("Do appends");
    for i in 0..10 {
        do_appends();
        println!("append: {}", i);
    }

    println!("Do iterators");
    for i in 0..10 {
        let count = do_iterations();
        println!("iterations: {}", i);
    }

    println!("Done");
}

fn do_appends() {
    let tempdir = tempdir().unwrap();
    let storage = FileSegmentStream::new(tempdir.path().to_path_buf(), 1024);
    let mut log = Log::new(RefCell::new(storage));

    let data = [0; MESSAGE_SIZE];

    for _ in 0..ITERATIONS {
        let _ = log.append(&data);
    }
}

fn do_iterations() -> i32 {
    let tempdir = tempdir().unwrap();
    let storage = FileSegmentStream::new(tempdir.path().to_path_buf(), 1024);
    let mut log = Log::new(RefCell::new(storage));

    let data = [0; MESSAGE_SIZE];

    for _ in 0..ITERATIONS {
        let _ = log.append(&data);
    }

    let mut count = 0;

    for _ in log.into_iter() {
        count += 1;
    }

    count
}

