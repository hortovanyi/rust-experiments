extern crate parquet;

use std::fs::File;
use std::path::Path;
use parquet::file::reader::SerializedFileReader;
use parquet::arrow::arrow_reader::ParquetFileArrowReader;
use parquet::arrow::ArrowReader;
use std::rc::Rc;

extern crate arrow;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::error::Result;
use arrow::record_batch::*;

fn get_arrow_reader(path: &str) -> ParquetFileArrowReader {
    let file = File::open(&Path::new(path)).unwrap();

    let reader = SerializedFileReader::new(file).unwrap();
    ParquetFileArrowReader::new(Rc::new(reader)) 
}

fn print_batch(batch: &RecordBatch) {
    let sec = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    let nanosec = batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
    let latitude = batch.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
    let longitude = batch.column(6).as_any().downcast_ref::<Float64Array>().unwrap();
    
    for i in 0..batch.num_rows() {
        println!("{}.{} ({},{})", sec.value(i), nanosec.value(i), latitude.value(i), longitude.value(i));
    }
}

fn print_parquet(path:&str) {
    let mut arrow_reader = get_arrow_reader(path);
    let schema = arrow_reader.get_schema().unwrap();

    println!("schema: {}", schema);

    let mut record_reader = arrow_reader.get_record_reader(100).expect("Failed to read record batch!");
    
    while let Some(batch) = record_reader.next_batch().expect("Failed to read record batch!") {
        print_batch(&batch)  
    }

}
fn main() {
    print_parquet("./data/parquet-arrow-nav-sat-fix-20200318132301.parquet"); 
}
