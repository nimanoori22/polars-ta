use crate::overlap::ema::{ema, EmaOptions};
use crate::utils::core::get_offset;
use crate::utils::error::CommandResult;
use polars::prelude::*;


pub struct DemaOptions {
    pub length: Option<i32>,
    pub offset: Option<i32>,
    pub fillna: bool,
}


impl Default for DemaOptions {
    fn default() -> Self {
        DemaOptions {
            length: Some(10),
            offset: None,
            fillna: false,
        }
    }
}


pub fn dema(
    close: &Series,
    options: DemaOptions
) -> CommandResult<Series> {
    let close = close.clone();

    let length = match options.length {
        Some(length) => {
            if length > 0 {
                length
            } else {
                14
            }
        },
        None => 14
    };

    let offset = get_offset(options.offset);

    let ema1 = ema(&close, EmaOptions {
        length: Some(length),
        ..Default::default()
    })?;

    let ema2 = ema(&ema1, EmaOptions {
        length: Some(length),
        ..Default::default()
    })?;

    let dema = ema1 * 2.0 - ema2;

    let dema = if offset != 0 {
        dema.shift(offset as i64)
    } else {
        dema
    };

    Ok(dema)
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
    fn test_dema() {
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

        let close = df.column("close").unwrap();
        let dema = dema(
            &close,
            DemaOptions {
                ..Default::default()
            }
        ).unwrap();
        println!("{:?}", dema)
    }
}