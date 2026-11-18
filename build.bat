@echo off
setlocal

cargo fmt -- --check
if errorlevel 1 goto :failed

cargo test
if errorlevel 1 goto :failed

cargo clippy --all-targets -- -D warnings
if errorlevel 1 goto :failed

cargo build --release
if errorlevel 1 goto :failed

echo Build and validation succeeded.
exit /b 0

:failed
echo Build or validation failed.
exit /b 1
