## [0.1.0] - Initial version

* Initial release.

## [0.1.1] - Change github repo

* Changed github repo

## [0.1.2] - Cleanups

* Added an example.
* Cleaned up the source code to improve static analysis.
* Added a little more documentation to the README.

## [0.1.3] - Bug Fix, PersistentMap

* Fixed a bug in `clear()` where counts were not updated.
* Added an implementation of `Map` that is disk-persistent.

## [0.1.4] - Cleanups

* Tweak pubspec, minor cleanup.

## [0.1.5] - Cleanups

* Tweak pubspec, minor cleanup.

## [0.1.6] - Null safety changes

* Changes to make the library null-safe.

## [0.1.7] - Clean ups and bug fixes

* Fixes to the block size calculation.
* Cleanups

## [0.1.8] - Clean ups and bug fixes

* Fixed README to align with null-safe API.
* Fixed pedantic errors.

## [0.2.1] - Robustness, readonly mode, compaction

* Added readonly mode with shared file locking (`readonly` parameter on `HashDBM` and `PersistentMap` factories).
* Added `compact()` method to truncate trailing free blocks and reclaim disk space.
* Added header CRC validation with `seal()`/`validate()` to detect header corruption.
* Added file locking (exclusive for read-write, shared for readonly) with a `Finalizer` safety net.
* Fixed `put()` to correctly return the previous value instead of the new value.
* Fixed `putIfAbsent()` to check for existing keys before inserting and to not call the callback when a value exists.
* Fixed `clear()` to reset `numBytes` to zero.
* Fixed `align()` to return the input unchanged when already aligned.
* Fixed memory pool `_write()` to overwrite stale pointers with `Pointer.NIL`.
* Optimised bucket flush with dirty-tracking to avoid rewriting the entire bucket array on every flush.
* Added `PointerBlock.writeAt()` for single-entry writes.
* Bumped version format to `0x00010009`.
* Bumped minimum SDK constraint to `>=2.17.0`.
* Removed stale Flutter/transitive dependencies from `pubspec.lock`.
* Test files now use distinct filenames to avoid collisions.
* Added regression test suite.

## [0.2.0] - Clean up

* Added a LICENSE files.
* Fixed some formatting issues.

## [0.3.0] - Versioned database, merge & flatten

* Added `VersionedHashDBM` — delta-overlay transactions with point-in-time snapshots.
* Added `Transaction` interface (`begin`, `commit`, `rollback`) for atomic versioned writes.
* Added `merge()` — compact all (or a range of) deltas into the base table.
* Added `flatten()` — merge all deltas and convert back to plain `HashDBM` format.
* Added `DeltaBlock` and `VersionStore` for on-disk delta storage.
* New exports: `VersionedDBM`, `VersionedHashDBM`, `Transaction`.


