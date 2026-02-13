use std::sync::PoisonError;

use bincode::ErrorKind;
// Custom Result type
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    Parse(String),
    Internal(String),
}

impl From<std::num::ParseIntError> for Error {
    fn from(value: std::num::ParseIntError) -> Self {
        Error::Parse(value.to_string())
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(value: std::num::ParseFloatError) -> Self {
        Error::Parse(value.to_string())
    }
}

// 需要实现这个trait，否则在mvcc.rs的lock获取中会报
// `?` couldn't convert the error to `error::Error`
impl<T> From<PoisonError<T>> for Error {
    fn from(value: PoisonError<T>) -> Self {
        Error::Internal(value.to_string())
    }
}

impl From<Box<ErrorKind>> for Error {
    fn from(value: Box<ErrorKind>) -> Self {
        Error::Internal(value.to_string())
    }
}