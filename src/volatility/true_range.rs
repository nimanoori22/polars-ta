use crate::utils::core::{get_drift, get_offset, non_zero_range};
use polars::prelude::*;
use crate::utils::error::CommandResult;
use crate::utils::math::abs;

pub fn true_range(
    datetime: &Series,
    high: &Series, 
    low: &Series, 
    close: &Series, 
    drift: Option<i32>, 
    offset: Option<i32>
) -> CommandResult<Series> {
    let drift = get_drift(drift);
    let offset = get_offset(offset);

    let high_low_range = non_zero_range(high, low)?;
    let prev_close = close.shift(drift as i64);

    let high_minus_prev_close = high - &prev_close;
    let prev_close_minus_low = &prev_close - low;

    let ranges = vec![
        abs(&high_low_range)?,
        abs(&high_minus_prev_close)?,
        abs(&prev_close_minus_low)?
    ];

    let df : DataFrame = DataFrame::new(ranges)?;
    let true_range = df
        .max_horizontal()
        .unwrap()
        .unwrap();

    // set the first drift values to NaN
    let true_range: Series = true_range
        .f64()
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(i, value)| {
            if i < drift as usize {
                Some(f64::NAN)
            } else {
                value
            }
        })
        .collect();

    let true_range = if offset != 0 {
        true_range.shift(offset as i64)
    } else {
        true_range
    };

    Ok(true_range)
}


mod tests {

    use crate::utils::data_loader::{
        csv_to_dataframe, 
        set_column_names, 
        combine_date_time, 
        convert_to_naive_datetime
    };

    use super::*;

    #[test]
    fn test_true_range() {
        let mut df = csv_to_dataframe(
            "data/AUDNZD1.csv", 
            false
        );
        let _ = set_column_names(
            &mut df, 
            vec!["date", "time", "open", "high", "low", "close", "volume"]
        );
        let df = combine_date_time(&df).unwrap();
        let df = convert_to_naive_datetime(
            &df, 
            "%Y.%m.%d %H:%M"
        ).unwrap();
        let high = df.column("high").unwrap();
        let low = df.column("low").unwrap();
        let close = df.column("close").unwrap();

        let result = true_range(
            &df.column("datetime").unwrap(),
            &high,
            &low, 
            &close, 
            Some(1), 
            Some(0)
        ).unwrap();
    }
}