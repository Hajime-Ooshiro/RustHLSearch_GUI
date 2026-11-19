use std::fmt;

#[derive(Debug)]
pub struct SearchError(String);

impl SearchError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for SearchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for SearchError {}

#[derive(Debug)]
pub enum AppError {
    InvalidInput(String),
    Io(std::io::Error),
    Json(serde_json::Error),
    Search(SearchError),
    ThreadPanicked,
}

impl fmt::Display for AppError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(message) => write!(formatter, "{message}"),
            Self::Io(error) => write!(formatter, "{error}"),
            Self::Json(error) => write!(formatter, "{error}"),
            Self::Search(error) => write!(formatter, "{error}"),
            Self::ThreadPanicked => write!(formatter, "検索スレッドが異常終了しました"),
        }
    }
}

impl std::error::Error for AppError {}

impl From<std::io::Error> for AppError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for AppError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl From<String> for AppError {
    fn from(error: String) -> Self {
        Self::Search(SearchError::new(error))
    }
}

impl From<Box<dyn std::error::Error>> for AppError {
    fn from(error: Box<dyn std::error::Error>) -> Self {
        Self::Search(SearchError::new(error.to_string()))
    }
}
