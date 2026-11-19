use log::{Level, Log, Metadata, Record};
use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::{Arc, Mutex};

pub const LOG_FILENAME: &str = "hlsearch.log";
pub const MAX_LOG_ENTRIES: usize = 500;

pub struct UiLogger {
    pub entries: Arc<Mutex<VecDeque<String>>>,
    pub writer: Mutex<BufWriter<File>>,
}

impl Log for UiLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= Level::Info
    }

    fn log(&self, record: &Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let entry = format!("[{}] {}", record.level(), record.args());
        eprintln!("{entry}");

        let mut writer = self
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Err(error) = writeln!(writer, "{entry}").and_then(|()| writer.flush()) {
            eprintln!("Failed to write log file: {error}");
        }

        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.len() == MAX_LOG_ENTRIES {
            entries.pop_front();
        }
        entries.push_back(entry);
    }

    fn flush(&self) {}
}
