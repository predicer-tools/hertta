use crate::{TimeLine, TimeStamp};
use std::iter;

pub fn check_stamps_match(
    checkable_series: &Vec<(TimeStamp, f64)>,
    time_line: &TimeLine,
    context: &str,
) -> Result<(), String> {
    if checkable_series.len() != time_line.len() {
        return Err(format!("{} length doesn't match time line", context));
    }
    for (forecast_pair, time_stamp) in iter::zip(checkable_series, time_line) {
        if forecast_pair.0 != *time_stamp {
            return Err(format!("{} time stamp doesn't match time line", context));
        }
    }
    Ok(())
}
