use polars::prelude::*;


pub fn abs(series: &Series) -> Result<Series, PolarsError> {
    let series = series.f64()?;
    let abs_series = series
        .apply_values(
            |value| value.abs()
        );
    Ok(abs_series.into_series())
}