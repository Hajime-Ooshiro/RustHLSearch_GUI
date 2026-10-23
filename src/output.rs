use chrono::Local;
use std::path::{Path, PathBuf};

/// 出力ファイルパスに深さとタイムスタンプ (YYYYMMDD_HHMMSS) を挿入する
pub fn with_timestamp(path: &Path, depth: usize) -> PathBuf {
    let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();

    let parent = path.parent();
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("output");
    let ext = path.extension().and_then(|s| s.to_str());

    let new_name = match ext {
        Some(ext) => format!("{}_depth{}_{}.{}", stem, depth, timestamp, ext),
        None => format!("{}_depth{}_{}", stem, depth, timestamp),
    };

    match parent {
        Some(p) if !p.as_os_str().is_empty() => p.join(new_name),
        _ => PathBuf::from(new_name),
    }
}

#[cfg(test)]
mod tests {
    use super::with_timestamp;
    use std::path::Path;

    #[test]
    fn timestamped_path_preserves_parent_and_extension() {
        let path = with_timestamp(Path::new("results/shift_path.json"), 8);
        let file_name = path.file_name().unwrap().to_str().unwrap();
        assert_eq!(path.parent(), Some(Path::new("results")));
        assert!(file_name.starts_with("shift_path_depth8_"));
        assert!(file_name.ends_with(".json"));
    }

    #[test]
    fn timestamped_path_handles_extensionless_file() {
        let path = with_timestamp(Path::new("shift_path"), 3);
        let file_name = path.file_name().unwrap().to_str().unwrap();
        assert!(file_name.starts_with("shift_path_depth3_"));
        assert!(path.extension().is_none());
    }
}
