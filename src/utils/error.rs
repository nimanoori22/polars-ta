use polars::prelude::PolarsError;
use thiserror::Error;
use anyhow::Result as AnyResult;

#[derive(Debug, Error)]
pub enum CommandError {
    #[error(transparent)]
    Polars(#[from] PolarsError),
    #[error("DataFrame not found")]
    DataFrameNotFound,
    #[error("{0}")]
    Other(String),
}


impl From<anyhow::Error> for CommandError {
    fn from(error: anyhow::Error) -> Self {
        CommandError::Other(error.to_string())
    }
}


pub type CommandResult<T, E = CommandError> = AnyResult<T, E>;


