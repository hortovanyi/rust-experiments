extern crate parquet;
extern crate coord_transforms;
extern crate nalgebra as na;

use coord_transforms::prelude::*;
use std::fs::File;
use std::path::Path;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{RowAccessor,ListAccessor};
use chrono::{DateTime, Utc, TimeZone};

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
    fn ecef(&self) -> Vector3<f64> {
        //Define ellipsoid for the Earth
        let geo_ellipsoid = geo_ellipsoid::geo_ellipsoid::new(geo_ellipsoid::WGS84_SEMI_MAJOR_AXIS_METERS,
        geo_ellipsoid::WGS84_FLATTENING);

        let lla_vec = Vector3::new(self.latitude.to_radians(), self.longitude.to_radians(), self.altitude);

        //Convert to Earth-Centered Earth-Fixed (ECEF)
        geo::lla2ecef(&lla_vec, &geo_ellipsoid)
    }

    fn ned(&self, lla_origin: &Vector3<f64>) -> Vector3<f64> {
        //Define ellipsoid for the Earth
        let geo_ellipsoid = geo_ellipsoid::geo_ellipsoid::new(geo_ellipsoid::WGS84_SEMI_MAJOR_AXIS_METERS,
        geo_ellipsoid::WGS84_FLATTENING);

        let lla_vec = Vector3::new(self.latitude.to_radians(), self.longitude.to_radians(), self.altitude);

        // convert to ned
        geo::lla2ned(&lla_origin, &lla_vec, &geo_ellipsoid)
    }
    
    fn enu(&self, lla_origin: &Vector3<f64>) -> Vector3<f64> {
        //Define ellipsoid for the Earth
        let geo_ellipsoid = geo_ellipsoid::geo_ellipsoid::new(geo_ellipsoid::WGS84_SEMI_MAJOR_AXIS_METERS,
        geo_ellipsoid::WGS84_FLATTENING);

        let lla_vec = Vector3::new(self.latitude.to_radians(), self.longitude.to_radians(), self.altitude);

        // convert to enu
        geo::lla2enu(&lla_origin, &lla_vec, &geo_ellipsoid)
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

    let lla_origin = Vector3::new((-27.997133 as f64).to_radians(), (153.420374 as f64).to_radians(), 10.0 as f64);
                                            
    let file = File::open(&Path::new(path)).unwrap();

    let reader = SerializedFileReader::new(file).unwrap();

    let mut iter = reader.get_row_iter(None).unwrap();

    while let Some(row) = iter.next(){
        let gps = NavSatFix::from(row);
        let ecef_vec = gps.ecef();
        let ned_vec = gps.ned(&lla_origin);
        let enu_vec = gps.enu(&lla_origin);
        println!("lat: {} lon: {} alt: {} ecef: {:?} ned: {:?} enu: {:?}", gps.latitude, gps.longitude, gps.altitude, ecef_vec.data, ned_vec.data, enu_vec.data);
    }

}

fn main() {
    print_parquet("./data/parquet-arrow-nav-sat-fix-20200318132301.parquet"); 
}
