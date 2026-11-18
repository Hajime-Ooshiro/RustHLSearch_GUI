use std::fmt;

#[derive(Debug)]
pub enum AppError {
    InvalidInput(String),
    Io(std::io::Error),
    Json(serde_json::Error),
    Search(String),
    ThreadPanicked,
}

impl fmt::Display for AppError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(message) => write!(formatter, "{message}"),
            Self::Io(error) => write!(formatter, "{error}"),
            Self::Json(error) => write!(formatter, "{error}"),
            Self::Search(message) => write!(formatter, "{message}"),
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
        Self::Search(error)
    }
}

impl From<Box<dyn std::error::Error>> for AppError {
    fn from(error: Box<dyn std::error::Error>) -> Self {
        Self::Search(error.to_string())
    }
}
