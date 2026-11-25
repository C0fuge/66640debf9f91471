// Test-only registration: production integration/module wiring is separate.
#[path = "../src/cache_file.rs"]
mod cache_file;

#[cfg(unix)]
mod unix {
    use super::cache_file::{CacheFile, CacheFileError, MAX_CACHE_FILE_BYTES};
    use std::{
        cell::Cell,
        ffi::CString,
        fs::{self, File, OpenOptions},
        io::{self, Write},
        os::unix::{ffi::OsStrExt, fs::{PermissionsExt, symlink}},
        path::{Path, PathBuf},
        time::{Duration, Instant, UNIX_EPOCH},
    };

    fn fixture() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let temporary = tempfile::tempdir().unwrap();
        // Test roots must themselves satisfy the no-symlink ancestry rule (also
        // on Unix systems whose standard temporary-directory spelling is an alias).
        let base = temporary.path().canonicalize().unwrap();
        let cache = base.join("cache");
        fs::create_dir(&cache).unwrap();
        let candidate = cache.join("file.bin");
        fs::write(&candidate, b"synthetic cache bytes").unwrap();
        (temporary, cache, candidate)
    }

    fn rejected(root: &Path, candidate: &Path) {
        assert!(CacheFile::open(root, candidate).is_err());
    }

    #[test]
    fn valid_held_read_has_only_safe_size_metadata() {
        let (_temporary, cache, candidate) = fixture();
        let mut source = CacheFile::open(&cache, &candidate).unwrap();
        let mut copied = Vec::new();
        assert_eq!(source.metadata().byte_len, 21);
        assert_eq!(source.copy_to(&mut copied, 21, || false).unwrap(), 21);
        assert_eq!(copied, b"synthetic cache bytes");
        // Re-reading uses the same held descriptor, explicitly rewound.
        let mut second = Vec::new();
        source.copy_to(&mut second, 21, || false).unwrap();
        assert_eq!(second, copied);
    }

    #[test]
    fn outside_state_traversal_relative_and_ambiguous_paths_are_rejected() {
        let (temporary, cache, _) = fixture();
        let state = temporary.path().canonicalize().unwrap().join("state");
        fs::create_dir(&state).unwrap();
        fs::write(state.join("session"), b"synthetic private state").unwrap();
        rejected(&cache, &state.join("session"));
        rejected(&cache, &cache.join("../state/session"));
        rejected(&cache, &cache.join("./file.bin"));
        rejected(&cache, Path::new("file.bin"));
        rejected(Path::new("cache"), &cache.join("file.bin"));
        rejected(Path::new("/"), &cache.join("file.bin"));
        rejected(&cache, &cache);
    }

    #[test]
    fn symlink_root_component_final_and_hardlinks_are_rejected() {
        let (temporary, cache, candidate) = fixture();
        let base = temporary.path().canonicalize().unwrap();
        let alias = base.join("alias");
        symlink(&cache, &alias).unwrap();
        rejected(&alias, &alias.join("file.bin"));
        let nested = cache.join("nested");
        fs::create_dir(&nested).unwrap();
        fs::write(nested.join("data"), b"nested").unwrap();
        symlink(&nested, cache.join("directory-link")).unwrap();
        rejected(&cache, &cache.join("directory-link/data"));
        symlink(&candidate, cache.join("file-link")).unwrap();
        rejected(&cache, &cache.join("file-link"));
        let state = base.join("session-placeholder");
        fs::write(&state, b"synthetic session bytes").unwrap();
        fs::hard_link(&state, cache.join("hardlink")).unwrap();
        rejected(&cache, &cache.join("hardlink"));
        fs::hard_link(&candidate, base.join("outside-link")).unwrap();
        rejected(&cache, &candidate);
    }

    #[test]
    fn directory_and_fifo_are_rejected_without_waiting_for_a_writer() {
        let (_temporary, cache, _) = fixture();
        let folder = cache.join("folder");
        fs::create_dir(&folder).unwrap();
        rejected(&cache, &folder);
        let fifo = cache.join("fifo");
        let name = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        // SAFETY: name is NUL-terminated and names only this isolated fixture.
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        let started = Instant::now();
        rejected(&cache, &fifo);
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn pathname_replacement_never_reads_replacement_bytes() {
        let (_temporary, cache, candidate) = fixture();
        let mut source = CacheFile::open(&cache, &candidate).unwrap();
        fs::rename(&candidate, cache.join("original-held-file")).unwrap();
        fs::write(&candidate, b"replacement is not the approved descriptor").unwrap();
        let mut copied = Vec::new();
        assert_eq!(source.copy_to(&mut copied, MAX_CACHE_FILE_BYTES, || false), Err(CacheFileError::Changed));
        assert!(copied.is_empty());
    }

    #[test]
    fn replaced_root_or_parent_is_detected_even_with_held_ancestor_descriptors() {
        let (temporary, cache, candidate) = fixture();
        let mut source = CacheFile::open(&cache, &candidate).unwrap();
        let base = temporary.path().canonicalize().unwrap();
        fs::rename(&cache, base.join("old-cache")).unwrap();
        fs::create_dir(&cache).unwrap();
        fs::write(&candidate, b"replacement").unwrap();
        let mut copied = Vec::new();
        assert_eq!(source.copy_to(&mut copied, MAX_CACHE_FILE_BYTES, || false), Err(CacheFileError::Changed));
        assert!(copied.is_empty());
    }

    #[test]
    fn caller_and_fixed_size_limits_fail_before_copying() {
        let (_temporary, cache, candidate) = fixture();
        let mut source = CacheFile::open(&cache, &candidate).unwrap();
        let mut copied = Vec::new();
        assert_eq!(source.copy_to(&mut copied, 20, || false), Err(CacheFileError::TooLarge));
        assert!(copied.is_empty());
        let sparse = cache.join("too-large");
        File::create(&sparse).unwrap().set_len(MAX_CACHE_FILE_BYTES + 1).unwrap();
        assert!(matches!(CacheFile::open(&cache, &sparse), Err(CacheFileError::TooLarge)));
    }

    #[test]
    fn same_file_metadata_change_and_new_hardlink_are_detected() {
        let (_temporary, cache, candidate) = fixture();
        let mut source = CacheFile::open(&cache, &candidate).unwrap();
        let mode = fs::metadata(&candidate).unwrap().permissions().mode();
        fs::set_permissions(&candidate, fs::Permissions::from_mode(mode ^ 0o040)).unwrap();
        assert_eq!(source.copy_to(&mut Vec::new(), MAX_CACHE_FILE_BYTES, || false), Err(CacheFileError::Changed));
        let mut source = CacheFile::open(&cache, &candidate).unwrap();
        fs::hard_link(&candidate, cache.join("new-link")).unwrap();
        assert_eq!(source.copy_to(&mut Vec::new(), MAX_CACHE_FILE_BYTES, || false), Err(CacheFileError::Changed));
    }

    #[test]
    fn same_size_rewrite_with_a_changed_timestamp_is_rejected() {
        let (_temporary, cache, candidate) = fixture();
        let mut source = CacheFile::open(&cache, &candidate).unwrap();
        let mut writer = OpenOptions::new().write(true).open(&candidate).unwrap();
        writer.write_all(b"Synthetic cache bytes").unwrap();
        writer.set_modified(UNIX_EPOCH + Duration::from_secs(1234)).unwrap();
        assert_eq!(source.copy_to(&mut Vec::new(), MAX_CACHE_FILE_BYTES, || false), Err(CacheFileError::Changed));
    }

    struct MutatingDestination {
        source: PathBuf,
        bytes: Vec<u8>,
        changed: bool,
    }
    impl Write for MutatingDestination {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            self.bytes.extend_from_slice(data);
            if !self.changed {
                self.changed = true;
                let file = OpenOptions::new().write(true).open(&self.source)?;
                file.set_len(file.metadata()?.len() + 1)?;
            }
            Ok(data.len())
        }
        fn flush(&mut self) -> io::Result<()> { Ok(()) }
    }

    #[test]
    fn mutation_during_copy_fails_and_partial_destination_must_be_discarded() {
        let (_temporary, cache, candidate) = fixture();
        fs::write(&candidate, vec![b'x'; 128 * 1024]).unwrap();
        let mut source = CacheFile::open(&cache, &candidate).unwrap();
        let mut destination = MutatingDestination { source: candidate, bytes: Vec::new(), changed: false };
        assert_eq!(source.copy_to(&mut destination, MAX_CACHE_FILE_BYTES, || false), Err(CacheFileError::Changed));
        assert_eq!(destination.bytes.len(), 64 * 1024);
    }

    #[test]
    fn cancellation_is_checked_between_bounded_chunks() {
        let (_temporary, cache, candidate) = fixture();
        fs::write(&candidate, vec![b'x'; 128 * 1024]).unwrap();
        let mut source = CacheFile::open(&cache, &candidate).unwrap();
        let checks = Cell::new(0);
        let mut destination = Vec::new();
        let result = source.copy_to(&mut destination, MAX_CACHE_FILE_BYTES, || {
            checks.set(checks.get() + 1);
            checks.get() > 2
        });
        assert_eq!(result, Err(CacheFileError::Cancelled));
        assert_eq!(destination.len(), 64 * 1024);
    }
}

#[cfg(not(unix))]
#[test]
fn other_platforms_are_explicitly_unsupported() {
    assert!(matches!(cache_file::CacheFile::open(std::path::Path::new("cache"), std::path::Path::new("cache/file")),
        Err(cache_file::CacheFileError::Unsupported)));
}
