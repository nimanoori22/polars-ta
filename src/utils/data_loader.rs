use polars::prelude::*;
use std::io::Error;
use polars::export::chrono::NaiveDateTime;

pub fn csv_to_dataframe(path: &str, has_header: bool) -> DataFrame {
    let mut df = CsvReader::from_path(path)
        .unwrap()
        .infer_schema(None)
        .has_header(has_header)
        .finish()
        .unwrap();
    df
}

pub fn set_column_names(df: &mut DataFrame, names: Vec<&str>) -> Result<(), PolarsError> {
    df.set_column_names(&names)?;
    Ok(())
}


pub fn combine_date_time(df: &DataFrame) -> Result<DataFrame, PolarsError> {
    let mut df = df.clone();
    let date = df.column("date").unwrap();
    let time = df.column("time").unwrap();

    // Combine date and time columns into one datetime column
    let datetime: Vec<_> = date.utf8()?
        .into_iter()
        .zip(time.utf8()?.into_iter())
        .map(|(date, time)| {
            let date = date.unwrap();
            let time = time.unwrap();
            format!("{} {}", date, time)
        }).collect();

    // datetime to series
    let datetime_series = Series::new("datetime", datetime);
    let _ = df.drop_in_place("date")?;
    let _ = df.drop_in_place("time")?;

    let df = df.with_column(datetime_series)?;
    Ok(df.clone())
}


pub fn convert_to_naive_datetime(df: &DataFrame, datetime_fmt: &str) -> Result<DataFrame, PolarsError> {
    let mut df = df.clone();
    let datetime = df.column("datetime").unwrap();
    let datetime: Vec<_> = datetime.utf8()?
        .into_iter()
        .map(|datetime_str| {
            datetime_str.map(|datetime_str| {
                let naive = NaiveDateTime::parse_from_str(datetime_str, datetime_fmt).unwrap();
                naive
            })
        }).collect();

    let datetime_series = Series::new("datetime", datetime);
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
        let mut df = csv_to_dataframe(path, false);
        let names = vec!["date", "time", "open", "high", "low", "close", "volume"];
        let _= set_column_names(&mut df, names);
        println!("{:?}", df);
    }

    #[test]
    fn test_combine_date_time() {
        let path = "data/AUDNZD1.csv";
        let mut df = csv_to_dataframe(path, false);
        let names = vec!["date", "time", "open", "high", "low", "close", "volume"];
        let _= set_column_names(&mut df, names);
        let df = combine_date_time(&df).unwrap();
        println!("{:?}", df);
    }

    #[test]
    fn test_convert_to_naive_datetime() {
        let path = "data/AUDNZD1.csv";
        let mut df = csv_to_dataframe(path, false);
        let names = vec!["date", "time", "open", "high", "low", "close", "volume"];
        let _= set_column_names(&mut df, names);
        let df = combine_date_time(&df).unwrap();
        let df = convert_to_naive_datetime(&df, "%Y.%m.%d %H:%M").unwrap();
        println!("{:?}", df);
    }
}