use polars::export::chrono::DateTime;
use polars::export::chrono::Utc;
use polars::prelude::*;


fn is_datetime64_any_dtype(s: &Column) -> bool {
    match s.dtype() {
        DataType::Date | DataType::Datetime(..) => true,
        _ => false,
    }
}


pub fn unix_timestamp_to_naive_datetime(timestamp: i64) -> DateTime<Utc> {
    let naive = DateTime::from_timestamp_millis(timestamp).unwrap();
    naive
}


fn extract_datetime_as_i64<'a>(v: &AnyValue<'a>) -> i64 {
    if let AnyValue::Datetime(d, _, _) = v {
        *d
    } else {
        panic!("not a datetime");
    }
}


fn is_datetime_ordered(df: &DataFrame, index_col: &str) -> Result<bool, PolarsError> {
    let df = df.clone();
    let index = df.column(index_col)?;
    let index_is_datetime = is_datetime64_any_dtype(index);

    let first: AnyValue<'_> = index.get(0)?;
    let first = extract_datetime_as_i64(&first);
    let last = index.get(index.len() - 1)?;
    let last = extract_datetime_as_i64(&last);

    let ordered = match (first, last) {
        (first, last) => first < last,
        _ => false,
    };

    Ok(index_is_datetime && ordered)
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
    fn test_is_datetime_ordered() {
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
        let is_ordered = is_datetime_ordered(
            &df, 
            "datetime"
        ).unwrap();
        assert_eq!(is_ordered, true);
    }

    #[test]
    fn test_is_datetime64_any_dtype() {
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
        let is_any_dtype = is_datetime64_any_dtype(
            &df.column("datetime").unwrap()
        );
        assert_eq!(is_any_dtype, true);
    }
}