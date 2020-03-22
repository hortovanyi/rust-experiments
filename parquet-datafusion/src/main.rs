extern crate arrow;
extern crate datafusion;

use arrow::array::{Float64Array, Int32Array, UInt32Array, StringArray};

use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;

fn main() -> Result<()>{
    // create local executuin context
    let mut ctx = ExecutionContext::new();

    let gps_data = "./data/parquet-arrow-nav-sat-fix-20200318132301.parquet";

    // register parquet file with the execution context
    ctx.register_parquet(
        "gps_data",
        gps_data
    )?;

    // simple selection
    let sql = "SELECT header.stamp.sec, header.stamp.nanosec, latitude, longitude WHERE status.status >= 1";

    // create the query plan
    let plan = ctx.create_logical_plan(&sql)?;
    let plan = ctx.optimize(&plan)?;
    let plan = ctx.create_physical_plan(&plan, 1024*1024)?;

    // execute the query
    let results = ctx.collect(plan.as_ref())?;

    // iterate over the results
    results.iter().for_each(|batch| {
        println!(
            "RecordBatch has {} rows and {} columns",
            batch.num_rows(),
            batch.num_columns()
        );
        let header_sec = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let header_nanosec = batch.column(1).as_any().downcast_ref::<UInt32Array>().unwrap();
        let latitude = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        let longitude = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();

        for i in 0..batch.num_rows() {
            println!("ts: {}.{} ({},{})", header_sec.value(i), header_nanosec.value(i), latitude.value(i), longitude.value(i));
        }

    });

    Ok(())

}
