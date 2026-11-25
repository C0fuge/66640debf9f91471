use super::*;
use std::{cell::Cell, fs, io, os::unix::fs::symlink, path::PathBuf};

fn fixture() -> (tempfile::TempDir, PathBuf, PathBuf, CacheFile) {
    let temporary = tempfile::tempdir().unwrap();
    let base = temporary.path().canonicalize().unwrap();
    let cache = base.join("cache");
    let output = base.join("chosen-directory");
    fs::create_dir(&cache).unwrap();
    fs::create_dir(&output).unwrap();
    let path = cache.join("source");
    fs::write(&path, b"synthetic native save bytes").unwrap();
    let source = CacheFile::open(&cache, &path).unwrap();
    (temporary, path, output.join("new-copy"), source)
}

#[test]
fn publishes_exact_unnamed_inode_and_reports_bounded_progress() {
    let (_temporary, _source_path, destination, mut source) = fixture();
    let expected = b"synthetic native save bytes";
    let progress = std::cell::RefCell::new(Vec::new());
    let commits = Cell::new(0);
    let bytes = publish(&mut source, &destination, 1024, || false,
        |done, total| {
            // No named temporary file can be exchanged before publication.
            assert_eq!(fs::read_dir(destination.parent().unwrap()).unwrap().count(), 0);
            progress.borrow_mut().push((done, total));
        },
        || { commits.set(commits.get() + 1); true }).unwrap();
    assert_eq!(bytes, expected.len() as u64);
    assert_eq!(fs::read(&destination).unwrap(), expected);
    assert_eq!(commits.get(), 1);
    assert_eq!(progress.borrow().first(), Some(&(0, bytes)));
    assert_eq!(progress.borrow().last(), Some(&(bytes, bytes)));
    assert_eq!(fs::read_dir(destination.parent().unwrap()).unwrap().count(), 1);
}

#[test]
fn existing_destination_and_racing_basename_never_get_overwritten() {
    let (_temporary, _source_path, destination, mut source) = fixture();
    fs::write(&destination, b"existing").unwrap();
    assert_eq!(publish(&mut source, &destination, 1024, || false, |_, _| {}, || true), Err(SaveFileError::Conflict));
    assert_eq!(fs::read(&destination).unwrap(), b"existing");
    fs::remove_file(&destination).unwrap();
    let result = publish(&mut source, &destination, 1024, || false, |_, _| {}, || {
        fs::write(&destination, b"racing writer").unwrap();
        true
    });
    assert_eq!(result, Err(SaveFileError::Conflict));
    assert_eq!(fs::read(&destination).unwrap(), b"racing writer");
}

#[test]
fn destination_symlink_components_and_final_symlink_are_rejected() {
    let (temporary, _source_path, destination, mut source) = fixture();
    let base = temporary.path().canonicalize().unwrap();
    symlink(destination.parent().unwrap(), base.join("alias")).unwrap();
    assert_eq!(publish(&mut source, &base.join("alias/copy"), 1024, || false, |_, _| {}, || true), Err(SaveFileError::Unavailable));
    let other = base.join("existing-file");
    fs::write(&other, b"never replace").unwrap();
    symlink(&other, &destination).unwrap();
    assert_eq!(publish(&mut source, &destination, 1024, || false, |_, _| {}, || true), Err(SaveFileError::Conflict));
    assert_eq!(fs::read(&other).unwrap(), b"never replace");
}

#[test]
fn invalid_and_missing_destination_parents_are_not_created() {
    let (temporary, _source_path, destination, mut source) = fixture();
    let missing = temporary.path().canonicalize().unwrap().join("missing/copy");
    for path in [PathBuf::from("relative"), PathBuf::from("file:///tmp/copy"), destination.parent().unwrap().join("../copy"), missing.clone()] {
        assert_eq!(publish(&mut source, &path, 1024, || false, |_, _| {}, || true), Err(SaveFileError::Unavailable));
    }
    assert!(!missing.parent().unwrap().exists());
}

#[test]
fn cancellation_and_rejected_commit_gate_leave_no_visible_file() {
    let (_temporary, _source_path, destination, mut source) = fixture();
    let cancel = Cell::new(false);
    let commits = Cell::new(0);
    let result = publish(&mut source, &destination, 1024, || cancel.get(),
        |done, _| { if done > 0 { cancel.set(true); } },
        || { commits.set(commits.get() + 1); true });
    assert_eq!(result, Err(SaveFileError::Cancelled));
    assert_eq!(commits.get(), 0);
    assert!(!destination.exists());
    assert_eq!(publish(&mut source, &destination, 1024, || false, |_, _| {}, || false), Err(SaveFileError::Cancelled));
    assert_eq!(fs::read_dir(destination.parent().unwrap()).unwrap().count(), 0);
}

#[test]
fn late_cancel_after_commit_gate_reports_actual_success() {
    let (_temporary, _source_path, destination, mut source) = fixture();
    let cancel = Cell::new(false);
    let result = publish(&mut source, &destination, 1024, || cancel.get(), |_, _| {}, || {
        cancel.set(true);
        true
    });
    assert!(result.is_ok());
    assert!(destination.exists());
}

#[test]
fn copy_limit_and_source_mutation_discard_the_unnamed_destination() {
    let (_temporary, source_path, destination, mut source) = fixture();
    assert_eq!(publish(&mut source, &destination, 1, || false, |_, _| {}, || true), Err(SaveFileError::TooLarge));
    let result = publish(&mut source, &destination, 1024, || false, |done, _| {
        if done > 0 { fs::write(&source_path, b"changed").unwrap(); }
    }, || true);
    assert_eq!(result, Err(SaveFileError::SourceChanged));
    assert_eq!(fs::read_dir(destination.parent().unwrap()).unwrap().count(), 0);
}

#[test]
fn changed_destination_parent_before_commit_is_a_safe_failure() {
    let (temporary, _source_path, destination, mut source) = fixture();
    let old = temporary.path().canonicalize().unwrap().join("old-directory");
    let moved = Cell::new(false);
    let committed = Cell::new(false);
    let result = publish(&mut source, &destination, 1024, || false, |done, _| {
        if done > 0 && !moved.replace(true) {
            fs::rename(destination.parent().unwrap(), &old).unwrap();
            fs::create_dir(destination.parent().unwrap()).unwrap();
        }
    }, || { committed.set(true); true });
    assert_eq!(result, Err(SaveFileError::Unavailable));
    assert!(!committed.get());
    assert_eq!(fs::read_dir(&old).unwrap().count(), 0);
    assert!(!destination.exists());
}

#[test]
fn directory_replacement_after_link_is_unknown_and_never_unlinks() {
    let (temporary, _source_path, destination, mut source) = fixture();
    let old = temporary.path().canonicalize().unwrap().join("old-directory");
    let result = linux::publish_with_sync(&mut source, &destination, 1024, || false, |_, _| {}, || true,
        |file, directory| {
            if directory {
                fs::rename(destination.parent().unwrap(), &old)?;
                fs::create_dir(destination.parent().unwrap())?;
            }
            file.sync_all()
        });
    assert_eq!(result, Err(SaveFileError::OutcomeUnknown));
    assert_eq!(fs::read(old.join("new-copy")).unwrap(), b"synthetic native save bytes");
    assert!(!destination.exists());
}

#[test]
fn sync_failures_distinguish_unpublished_error_from_post_link_uncertainty() {
    let (_temporary, _source_path, destination, mut source) = fixture();
    let error = linux::publish_with_sync(&mut source, &destination, 1024, || false, |_, _| {}, || true,
        |_, _| Err(io::Error::other("synthetic temp sync failure")));
    assert_eq!(error, Err(SaveFileError::Io));
    assert_eq!(fs::read_dir(destination.parent().unwrap()).unwrap().count(), 0);
    let error = linux::publish_with_sync(&mut source, &destination, 1024, || false, |_, _| {}, || true,
        |file, directory| if directory { Err(io::Error::other("synthetic directory sync failure")) } else { file.sync_all() });
    assert_eq!(error, Err(SaveFileError::OutcomeUnknown));
    assert_eq!(fs::read(&destination).unwrap(), b"synthetic native save bytes");
    // A new explicit attempt encounters the committed name, not an overwrite.
    assert_eq!(publish(&mut source, &destination, 1024, || false, |_, _| {}, || true), Err(SaveFileError::Conflict));
}

#[test]
fn source_mutation_during_temp_sync_is_rejected_before_visibility() {
    let (_temporary, source_path, destination, mut source) = fixture();
    let committed = Cell::new(false);
    let error = linux::publish_with_sync(&mut source, &destination, 1024, || false, |_, _| {},
        || { committed.set(true); true },
        |file, directory| {
            assert!(!directory);
            file.sync_all()?;
            fs::write(&source_path, b"mutated during sync")?;
            Ok(())
        });
    assert_eq!(error, Err(SaveFileError::SourceChanged));
    assert!(!committed.get());
    assert_eq!(fs::read_dir(destination.parent().unwrap()).unwrap().count(), 0);
}
