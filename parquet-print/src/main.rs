extern crate parquet;

use std::fs::File;
use std::path::Path;
use parquet::file::reader::{FileReader, SerializedFileReader};

fn print_parquet(path:&str ) {
    let file = File::open(&Path::new(path)).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let mut iter = reader.get_row_iter(None).unwrap();

    while let Some(record) = iter.next() {
        println!("{}", record);
    }
}
fn main() {
    print_parquet("./data/parquet-arrow-nav-sat-fix-20200318132301.parquet"); 
}


