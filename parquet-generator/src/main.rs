#![feature(generators, generator_trait)]
extern crate parquet;

use std::fs::File;
use std::path::Path;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{RowAccessor,ListAccessor};
use chrono::{DateTime, Utc, TimeZone};


use std::ops::{Generator, GeneratorState};
use std::pin::Pin;

#[derive(Debug, Clone)]
struct NavSatFix {
    stamp_sec: i32, 
    stamp_nanosec: i64,
    timestamp: DateTime<Utc>,
    frame_id: String,
    status: i8,
    service: u16,
    latitude: f64,
    longitude: f64,
    altitude: f64,
    position_covariance: [f64;9],
    position_covariance_type: u8,
}

impl NavSatFix {
    fn new(stamp_sec: i32, 
        stamp_nanosec: i64,
        frame_id: String,
        status: i8,
        service: u16,
        latitude: f64,
        longitude: f64,
        altitude: f64,
        position_covariance: [f64;9],
        position_covariance_type: u8)  -> NavSatFix {
        NavSatFix {stamp_sec: stamp_sec, 
                   stamp_nanosec: stamp_nanosec,
                   timestamp: Utc.timestamp(stamp_sec.into(), stamp_nanosec as u32),
                    frame_id: frame_id,
                    status: status,
                    service: service,
                    latitude: latitude,
                    longitude: longitude,
                    altitude: altitude,
                    position_covariance: position_covariance,
                    position_covariance_type: position_covariance_type}
    }
}

impl From <parquet::record::Row> for NavSatFix {
    fn from (row: parquet::record::Row ) -> Self {
        let sec = row.get_int(0).unwrap();
        let nanosec = row.get_long(1).unwrap();
        let frame = row.get_string(2).unwrap().to_string();
        let status = row.get_byte(3).unwrap();
        let service = row.get_ushort(4).unwrap();
        let latitude = row.get_double(5).unwrap();
        let longitude = row.get_double(6).unwrap();
        let altitude = row.get_double(7).unwrap();
        let mut covar: [f64;9] = [0.0;9];
        let covar_row = row.get_list(8).unwrap();
        for i in 0 .. covar_row.len() {
            covar[i]=covar_row.get_double(i).unwrap();
        }
        let covar_type = row.get_ubyte(9).unwrap();

        NavSatFix::new(sec, nanosec, frame, status, service, 
            latitude, longitude, altitude, covar, covar_type)
    }
}

fn print_parquet(path:&str) {

    let mut gps_generator = || {
        let file = File::open(&Path::new(path)).unwrap();

        let reader = SerializedFileReader::new(file).unwrap();

        let mut iter = reader.get_row_iter(None).unwrap();

        // let mut gps_vec = Vec::<NavSatFix>::new();
        
        while let Some(row) = iter.next(){
            let gps = NavSatFix::from(row);
            // gps_vec.push(gps);
            yield gps.clone();
        }

        // for gps in &gps_vec {
            // yield *gps;
        // }
        return "done"
    };

    loop {

        match Pin::new(&mut gps_generator).resume(()){
            GeneratorState::Yielded(gps) => {
                println!("{:?}", gps);
            }
            GeneratorState::Complete("done") => {
                break;
            }
            _ => panic!("unexpected value from resume")
        }
    }

}
fn main() {
    print_parquet("./data/parquet-arrow-nav-sat-fix-20200318132301.parquet"); 
}
