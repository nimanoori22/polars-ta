use polars::prelude::*;
use crate::utils::error::CommandResult;
use polars::export::chrono::{DateTime, Utc};

pub fn get_drift(drift: Option<i32>) -> i32 {
    match drift {
        Some(d) if d > 0 => d,
        _ => 1,
    }
}


pub fn get_offset(x: Option<i32>) -> i32 {
    match x {
        Some(val) if val.is_positive() => val,
        _ => 0,
    }
}


fn to_i64<'a>(v: &AnyValue<'a>) -> i64 {
    if let AnyValue::Int64(i) = v {
        *i
    } else {
        panic!("not an i64");
    }
}


fn is_percent(x: Option<f64>) -> bool {
    match x {
        Some(val) if val >= 0.0 && val <= 100.0 => true,
        _ => false,
    }
}


pub fn non_zero_range(high: &Column, low: &Column) -> CommandResult<Series> {
    let diff = match high - low {
        Ok(diff) => diff,
        Err(_) => return Err("Failed to calculate difference".into()),
    };
    // diff to vector
    let diff = diff.f64()?.into_iter().collect::<Vec<_>>();
    // check if any value is zero and add epsilon if true
    let diff = diff.into_iter().map(|val| {
        if val == Some(0.0) {
            Some(val.unwrap() + f64::EPSILON)
        } else {
            val
        }
    }).collect::<Vec<_>>();
    // vector to series
    let diff = Series::new("diff".into(), diff);
    Ok(diff)
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
    fn test_get_drift() {
        assert_eq!(get_drift(Some(1)), 1);
        assert_eq!(get_drift(Some(0)), 1);
        assert_eq!(get_drift(Some(-1)), 1);
        assert_eq!(get_drift(None), 1);
    }

    #[test]
    fn test_get_offset() {
        assert_eq!(get_offset(Some(1)), 1);
        assert_eq!(get_offset(Some(0)), 0);
        assert_eq!(get_offset(Some(-1)), 0);
        assert_eq!(get_offset(None), 0);
    }

    #[test]
    fn test_is_percent() {
        assert_eq!(is_percent(Some(0.0)), true);
        assert_eq!(is_percent(Some(100.0)), true);
        assert_eq!(is_percent(Some(50.0)), true);
        assert_eq!(is_percent(Some(-1.0)), false);
        assert_eq!(is_percent(Some(101.0)), false);
        assert_eq!(is_percent(None), false);
    }

    #[test]
    fn test_non_zero_range() {
        let mut df = csv_to_dataframe(
            "data/AUDNZD1.csv", 
            false
        ).unwrap();
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
        let diff = non_zero_range(high, low).unwrap();
        println!("{:?}", diff);
    }
}