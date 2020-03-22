# rust-experiments
Small projects to test rust capabilities.

All crates use the parquet file located in the data directory.

These small experiments are testing the maturity of capabilities in the rust version of Apache Arrow. Presently targetted against 0.16.0. There is also a current limitation requiring rust nightly. This means that rust language server doesnt work.

The parquet files has gps data collected whilst driving. It has the following schema based on the ROS2 nav-sat-fix message. The structs were flattened and hence the names are dotted eg header.stamp.sec 

### parquet schema
```
Metadata for file: parquet-arrow-nav-sat-fix-20200318132301.parquet

version: 1
num of rows: 627
created by: parquet-cpp version 1.5.1-SNAPSHOT
message schema {
  OPTIONAL INT32 header.stamp.sec;
  OPTIONAL INT64 header.stamp.nanosec;
  OPTIONAL BYTE_ARRAY header.frame_id (UTF8);
  OPTIONAL INT32 status.status (INT_8);
  OPTIONAL INT32 status.service (UINT_16);
  OPTIONAL DOUBLE latitude;
  OPTIONAL DOUBLE longitude;
  OPTIONAL DOUBLE altitude;
  OPTIONAL group position_covariance (LIST) {
    REPEATED group list {
      OPTIONAL DOUBLE item;
    }
  }
  OPTIONAL INT32 position_covariance_type (UINT_8);
}
```

## parquet-print
reads and prints the parquet file in the data directory 
```
cargo run -p parquet-print 
```

## parquet-datafusion
Loads up the parquet file in the data directory and performs a select sql query
```
cargo run -p parquet-datafusion
```
This does not run at present as the sql parser is unable to parse the sql query `SELECT header.stamp.sec, header.stamp.nanosec, latitude, longitude WHERE status.status >= 1`

