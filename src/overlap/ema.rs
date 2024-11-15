use std::process::Command;

use crate::utils::core::get_offset;
use polars::prelude::*;
use crate::utils::error::CommandResult;

pub struct EmaOptions {
    pub length: Option<i32>,
    pub adjust: bool,
    pub mamode: Option<String>,
    pub offset: Option<i32>,
    pub fillna: bool,
}

impl Default for EmaOptions {
    fn default() -> Self {
        EmaOptions {
            length: Some(10),
            adjust: false,
            mamode: Some("sma".to_string()),
            offset: None,
            fillna: false,
        }
    }
}


pub fn ema(
    close: &Series,
    options: EmaOptions
) -> CommandResult<Series> {
    let mut close = close.clone();

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

    let mamode = match options.mamode {
        Some(mamode) => {
            if mamode == "sma" || mamode == "ema" {
                mamode
            } else {
                "ema".to_string()
            }
        },
        None => "ema".to_string()
    };

    let offset = get_offset(options.offset);

    let alpha = 2.0 / (length as f64 + 1.0);

    let ewm_options = EWMOptions {
        alpha: alpha,
        adjust: options.adjust,
        bias: false,
        min_periods: 0,
        ignore_nulls: false,
    };
    
    if mamode == "sma" {
        let items_to_nth: Series = close.slice(0, length as usize);
        let sma_nth: Option<f64> = items_to_nth.mean();
        // let nan_vec = vec![f64::NAN; (length - 1) as usize];
        let nones: Vec<Option<f64>> = vec![None; (length - 1) as usize];
        let rest_of_close = close
            .slice(
                (length - 1) as i64, 
                close.len()
            );
        let rest_of_close_iter = rest_of_close
            .f64()
            .unwrap()
            .into_iter()
            .flatten()
            .map(Some);
        
        // combine the NaN vector with the rest of the values
        let mut new_close = nones
            .into_iter()
            .chain(rest_of_close_iter)
            .collect::<Vec<Option<f64>>>();
        
        new_close[length as usize - 1] = sma_nth;
        close = Series::new("close".into(), new_close);
    }
    
    let ema : Series = ewm_mean(
        &close,
        ewm_options
    )?;

    let ema = if offset != 0 {
        ema.shift(offset as i64)
    } else {
        ema
    };

    Ok(ema)   
}



mod tests {

    use crate::utils::data_loader::{
        csv_to_dataframe, 
        set_column_names, 
        combine_date_time, 
        convert_to_naive_datetime
    };
    use polars::prelude::abs;
    use polars::prelude::*;
    use super::ema;
    use super::EmaOptions;


    #[test]
    fn test_ema() {
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

        let close = df.column("close").unwrap().as_series().unwrap();
        let ema = ema(
            &close,
            EmaOptions {
                mamode: Some("ema".to_string()),
                ..Default::default()
            }
        ).unwrap();
        println!("{:?}", ema)
    }

    #[test]
    fn test_ema_sma() {
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

        let close = df.column("close").unwrap().as_series().unwrap();
        let ema = ema(
            &close,
            EmaOptions {
                mamode: Some("sma".to_string()),
                offset: Some(2),
                ..Default::default()
            }
        ).unwrap();
        println!("{:?}", ema)
    }
}