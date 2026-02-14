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

//! DMA file descriptor with O_DIRECT support and automatic fallback.
//!
//! `DmaFile` wraps an `OwnedFd` opened with `O_DIRECT` when possible,
//! bypassing the kernel page cache. If the filesystem doesn't support
//! O_DIRECT (e.g. tmpfs), it falls back to standard I/O. io_uring works
//! fine without O_DIRECT — alignment is unnecessary but harmless.
//! On non-Linux unix targets (for example macOS), `O_DIRECT` may be unavailable;
//! in that case this module opens files without direct I/O.

use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::{Path, PathBuf};

/// A file descriptor opened with O_DIRECT (with automatic fallback).
///
/// Uses `libc::open()` directly because `std::fs::OpenOptions` does not
/// expose the `O_DIRECT` flag. If O_DIRECT fails with EINVAL (filesystem
/// doesn't support it), retries without O_DIRECT.
///
/// The file descriptor is owned and will be closed on drop.
pub struct DmaFile {
    fd: OwnedFd,
    path: PathBuf,
    file_size: u64,
}

#[cfg(any(target_os = "linux", target_os = "android"))]
const O_DIRECT_FLAG: libc::c_int = libc::O_DIRECT;
#[cfg(not(any(target_os = "linux", target_os = "android")))]
const O_DIRECT_FLAG: libc::c_int = 0;

impl DmaFile {
    /// Open an existing file for reading with O_DIRECT (fallback to standard I/O).
    pub fn open_read(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let c_path = path_to_cstring(path)?;

        // Try O_DIRECT first
        let flags = libc::O_RDONLY | O_DIRECT_FLAG;
        let fd = unsafe { libc::open(c_path.as_ptr(), flags) };
        let fd = if fd < 0 {
            let err = io::Error::last_os_error();
            if O_DIRECT_FLAG != 0 && err.raw_os_error() == Some(libc::EINVAL) {
                // O_DIRECT not supported — retry without it
                let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY) };
                if fd < 0 {
                    return Err(io::Error::last_os_error());
                }
                fd
            } else {
                return Err(err);
            }
        } else {
            fd
        };

        // SAFETY: fd is valid, we just opened it.
        let owned_fd = unsafe { OwnedFd::from_raw_fd(fd) };
        let file_size = file_size_from_fd(owned_fd.as_raw_fd())?;
        Ok(Self {
            fd: owned_fd,
            path: path.to_path_buf(),
            file_size,
        })
    }

    /// Open or create a file for writing with O_DIRECT (fallback to standard I/O).
    pub fn open_write(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let c_path = path_to_cstring(path)?;
        let mode = 0o644;

        // Try O_DIRECT first
        let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC | O_DIRECT_FLAG;
        let fd = unsafe { libc::open(c_path.as_ptr(), flags, mode) };
        let fd = if fd < 0 {
            let err = io::Error::last_os_error();
            if O_DIRECT_FLAG != 0 && err.raw_os_error() == Some(libc::EINVAL) {
                // O_DIRECT not supported — retry without it
                let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC;
                let fd = unsafe { libc::open(c_path.as_ptr(), flags, mode) };
                if fd < 0 {
                    return Err(io::Error::last_os_error());
                }
                fd
            } else {
                return Err(err);
            }
        } else {
            fd
        };

        let owned_fd = unsafe { OwnedFd::from_raw_fd(fd) };
        Ok(Self {
            fd: owned_fd,
            path: path.to_path_buf(),
            file_size: 0,
        })
    }

    /// Open or create a file for writing without O_DIRECT.
    ///
    /// This is useful for workloads that need unaligned writes while still using
    /// io_uring for async submission/completion.
    pub fn open_write_buffered(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let c_path = path_to_cstring(path)?;
        let mode = 0o644;
        let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC;
        let fd = unsafe { libc::open(c_path.as_ptr(), flags, mode) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        let owned_fd = unsafe { OwnedFd::from_raw_fd(fd) };
        Ok(Self {
            fd: owned_fd,
            path: path.to_path_buf(),
            file_size: 0,
        })
    }

    /// Open or create a file for reading and writing with O_DIRECT (fallback to standard I/O).
    pub fn open_read_write(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let c_path = path_to_cstring(path)?;
        let mode = 0o644;

        // Try O_DIRECT first
        let flags = libc::O_RDWR | libc::O_CREAT | O_DIRECT_FLAG;
        let fd = unsafe { libc::open(c_path.as_ptr(), flags, mode) };
        let fd = if fd < 0 {
            let err = io::Error::last_os_error();
            if O_DIRECT_FLAG != 0 && err.raw_os_error() == Some(libc::EINVAL) {
                // O_DIRECT not supported — retry without it
                let flags = libc::O_RDWR | libc::O_CREAT;
                let fd = unsafe { libc::open(c_path.as_ptr(), flags, mode) };
                if fd < 0 {
                    return Err(io::Error::last_os_error());
                }
                fd
            } else {
                return Err(err);
            }
        } else {
            fd
        };

        let owned_fd = unsafe { OwnedFd::from_raw_fd(fd) };
        let file_size = file_size_from_fd(owned_fd.as_raw_fd())?;
        Ok(Self {
            fd: owned_fd,
            path: path.to_path_buf(),
            file_size,
        })
    }

    /// Get the raw file descriptor (for io_uring submission).
    #[inline]
    pub fn fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    /// Get the file size at open time.
    #[inline]
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Get the file path.
    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Update the cached file size (after writes).
    pub fn refresh_file_size(&mut self) -> io::Result<u64> {
        self.file_size = file_size_from_fd(self.fd.as_raw_fd())?;
        Ok(self.file_size)
    }
}

impl std::fmt::Debug for DmaFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DmaFile")
            .field("fd", &self.fd.as_raw_fd())
            .field("path", &self.path)
            .field("file_size", &self.file_size)
            .finish()
    }
}

/// Convert a Path to a CString for libc calls.
fn path_to_cstring(path: &Path) -> io::Result<std::ffi::CString> {
    use std::os::unix::ffi::OsStrExt;
    std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains null byte"))
}

/// Get file size via fstat.
fn file_size_from_fd(fd: RawFd) -> io::Result<u64> {
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::fstat(fd, &mut stat) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(stat.st_size as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_dma_file_write_and_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_dma.dat");

        // Create a file with O_DIRECT for writing (falls back on tmpfs)
        let f = DmaFile::open_write(&path).unwrap();
        assert_eq!(f.file_size(), 0);
        assert!(f.fd() >= 0);
        assert_eq!(f.path(), path);
    }

    #[test]
    fn test_dma_file_open_nonexistent() {
        let result = DmaFile::open_read("/tmp/nonexistent_tisql_test_file_xyz");
        assert!(result.is_err());
    }

    #[test]
    fn test_path_to_cstring() {
        let path = Path::new("/tmp/test");
        let c = path_to_cstring(path).unwrap();
        assert_eq!(c.to_bytes(), b"/tmp/test");
    }
}
