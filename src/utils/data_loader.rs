use polars::prelude::*;
use polars::export::chrono::NaiveDateTime;
use std::path::{self, Path};


pub fn csv_to_dataframe(path: &str, has_header: bool) -> PolarsResult<DataFrame> {
    let path: Option<path::PathBuf> = Some(Path::new(path).to_path_buf());
    CsvReadOptions::default()
            .with_has_header(has_header)
            .try_into_reader_with_file_path(path)?
            .finish()
}

pub fn set_column_names(df: &mut DataFrame, names: Vec<&str>) -> Result<(), PolarsError> {
    df.set_column_names(names)?;
    Ok(())
}


pub fn combine_date_time(df: &DataFrame) -> Result<DataFrame, PolarsError> {
    let mut df = df.clone();
    let date = df.column("date").unwrap();
    let time = df.column("time").unwrap();

    // Combine date and time columns into one datetime column
    let datetime: Vec<_> = date
        .str()?
        .into_iter()
        .zip(time.str()?)
        .map(|(date, time)| {
            let date = match date {
                Some(date) => date,
                None => panic!("Failed to get date")
            };
            let time = match time {
                Some(time) => time,
                None => panic!("Failed to get time")
            };
            let datetime = format!("{} {}", date, time);
            datetime
        }).collect();

    // datetime to series
    let datetime_series = Series::new("datetime".into(), datetime);
    let _ = df.drop_in_place("date")?;
    let _ = df.drop_in_place("time")?;

    let df = df.with_column(datetime_series)?;
    Ok(df.clone())
}


pub fn convert_to_naive_datetime(df: &DataFrame, datetime_fmt: &str) -> Result<DataFrame, PolarsError> {
    let mut df = df.clone();
    let datetime = df.column("datetime").unwrap();
    let datetime: Vec<NaiveDateTime> = datetime.str()?
        .into_iter()
        .map(|datetime_str| {
            if let Some(datetime_str) = datetime_str {
                let naive = NaiveDateTime::parse_from_str(datetime_str, datetime_fmt).unwrap();
                naive
            } else {
                panic!("Failed to parse datetime")
            }
        }).collect();

    let datetime_series = Series::new("datetime".into(), datetime);
    df.replace("datetime", datetime_series)?;
    Ok(df)
}


mod tests {
    use super::*;

    #[test]
    fn test_csv_to_dataframe() {
        let path = "data/AUDNZD1.csv";
        let df = csv_to_dataframe(path, false);
        println!("{:?}", df);
    }

    #[test]
    fn test_set_column_names() {
        let path = "data/AUDNZD1.csv";
        let mut df = match csv_to_dataframe(path, false) {
            Ok(df) => df,
            Err(e) => panic!("Error: {}", e),
        };
        let names = vec!["date", "time", "open", "high", "low", "close", "volume"];
        let _= set_column_names(&mut df, names);
        println!("{:?}", df);
    }

    #[test]
    fn test_combine_date_time() {
        let path = "data/AUDNZD1.csv";
        let mut df = match csv_to_dataframe(path, false) {
            Ok(df) => df,
            Err(e) => panic!("Error: {}", e),
        };
        let names = vec!["date", "time", "open", "high", "low", "close", "volume"];
        let _= set_column_names(&mut df, names);
        let df = combine_date_time(&df).unwrap();
        println!("{:?}", df);
    }

    #[test]
    fn test_convert_to_naive_datetime() {
        let path = "data/AUDNZD1.csv";
        let mut df = match csv_to_dataframe(path, false) {
            Ok(df) => df,
            Err(e) => panic!("Error: {}", e),
        };
        let names = vec!["date", "time", "open", "high", "low", "close", "volume"];
        let _= set_column_names(&mut df, names);
        let df = combine_date_time(&df).unwrap();
        let df = convert_to_naive_datetime(&df, "%Y.%m.%d %H:%M").unwrap();
        println!("{:?}", df);
    }
}