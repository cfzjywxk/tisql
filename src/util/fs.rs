// Copyright 2024 TiSQL Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Filesystem utilities for crash-safe operations.
//!
//! On POSIX systems, file creation and rename operations may not be durable
//! until the parent directory is fsynced. This module provides utilities
//! for ensuring crash-safe filesystem operations.
//!
//! ## Background
//!
//! When you create a file and fsync it, only the file contents are guaranteed
//! to be durable. The directory entry (the file's existence in its parent
//! directory) may not be durable until the parent directory is also fsynced.
//!
//! Similarly, when you rename a file, the rename operation may not be durable
//! until both the source and destination directories are fsynced.
//!
//! ## References
//!
//! - [RocksDB's sync_file_range_write](https://github.com/facebook/rocksdb/blob/main/env/io_posix.cc)
//! - [TiKV's sync_dir](https://github.com/tikv/tikv/blob/master/components/file_system/src/lib.rs)
//! - [PostgreSQL's fsync documentation](https://www.postgresql.org/docs/current/wal-reliability.html)

use std::fs::File;
use std::io;
use std::path::Path;

/// Fsync a directory to ensure directory entries are durable.
///
/// This is necessary after:
/// - Creating a new file (to make the file's existence durable)
/// - Renaming a file (to make the rename durable)
/// - Deleting a file (to make the deletion durable)
///
/// # Platform Notes
///
/// - On Linux/Unix: Opens the directory and calls fsync(2)
/// - On Windows: This is a no-op (NTFS handles this differently)
///
/// # Errors
///
/// Returns an error if the directory cannot be opened or fsynced.
pub fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        let f = File::open(dir)?;
        f.sync_all()?;
    }

    #[cfg(not(unix))]
    {
        // On Windows, directory fsync is not needed in the same way.
        // NTFS provides different guarantees.
        let _ = dir;
    }

    Ok(())
}

/// Fsync a file and its parent directory.
///
/// Use this after creating a new file to ensure both the file contents
/// and the file's existence are durable.
///
/// # Arguments
///
/// * `file` - The file to sync (must be open for writing)
/// * `path` - Path to the file (used to determine parent directory)
///
/// # Errors
///
/// Returns an error if fsync fails on either the file or directory.
pub fn sync_file_and_dir(file: &File, path: &Path) -> io::Result<()> {
    // Sync file contents
    file.sync_all()?;

    // Sync parent directory
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }

    Ok(())
}

/// Atomically rename a file with full durability.
///
/// This performs a rename operation that is crash-safe:
/// 1. The source file should already be fsynced
/// 2. Rename the file
/// 3. Fsync the destination directory (and source if different)
///
/// # Arguments
///
/// * `from` - Source path
/// * `to` - Destination path
///
/// # Errors
///
/// Returns an error if rename or directory fsync fails.
pub fn rename_durable(from: &Path, to: &Path) -> io::Result<()> {
    // Perform the rename
    std::fs::rename(from, to)?;

    // Fsync destination directory
    if let Some(dest_dir) = to.parent() {
        sync_dir(dest_dir)?;
    }

    // If source is in a different directory, fsync it too
    if let (Some(src_dir), Some(dest_dir)) = (from.parent(), to.parent()) {
        if src_dir != dest_dir {
            sync_dir(src_dir)?;
        }
    }

    Ok(())
}

/// Create a directory and fsync its parent to make creation durable.
///
/// # Arguments
///
/// * `path` - Path to the directory to create
///
/// # Errors
///
/// Returns an error if directory creation or parent fsync fails.
pub fn create_dir_durable(path: &Path) -> io::Result<()> {
    std::fs::create_dir_all(path)?;

    // Fsync parent to make the new directory entry durable
    if let Some(parent) = path.parent() {
        if parent.exists() {
            sync_dir(parent)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use tempfile::tempdir;

    #[test]
    fn test_sync_dir() {
        let dir = tempdir().unwrap();
        sync_dir(dir.path()).unwrap();
    }

    #[test]
    fn test_sync_file_and_dir() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.txt");

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        std::io::Write::write_all(&mut &file, b"hello").unwrap();
        sync_file_and_dir(&file, &path).unwrap();
    }

    #[test]
    fn test_rename_durable() {
        let dir = tempdir().unwrap();
        let from = dir.path().join("from.txt");
        let to = dir.path().join("to.txt");

        std::fs::write(&from, b"hello").unwrap();
        // Sync the source file first
        File::open(&from).unwrap().sync_all().unwrap();

        rename_durable(&from, &to).unwrap();

        assert!(!from.exists());
        assert!(to.exists());
        assert_eq!(std::fs::read(&to).unwrap(), b"hello");
    }

    #[test]
    fn test_create_dir_durable() {
        let dir = tempdir().unwrap();
        let new_dir = dir.path().join("subdir");

        create_dir_durable(&new_dir).unwrap();
        assert!(new_dir.exists());
    }
}
