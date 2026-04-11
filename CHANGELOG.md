# Changelog

All notable changes to this project will be documented in this file.

## [1.0.2] - 2026-04-08

### Fixed

- Fixed per-sitemap filename readability by including cleaner query-based hints for remote sources and parent-directory/file-stem hints for local sources.

### Changed

- Adjusted the human-readable filename prefix for per-sitemap outputs while keeping the trailing hash derived from the full original source string.
- Updated README examples and output file documentation to reflect the more identifiable filename format.

## [1.0.1] - 2026-04-08

### Fixed

- Fixed per-sitemap output filename collisions for child sitemap URLs that share the same netloc/path but differ by query string.
- Fixed silent overwrites between query-distinct child sitemap outputs by deriving filenames from a readable source-based base plus a short hash of the full source URL.
- Fixed per-sitemap outputs so each file contains only URLs extracted from that specific sitemap source.
- Fixed per-file URL exports to deduplicate entries before writing.
- Fixed `--stealth` behavior so it always forces `max_workers=1` instead of only warning about reduced stealth.
- Fixed directory scanning for `--directory` inputs so only `.xml` and `.xml.gz` files are matched.
- Fixed save directory handling so the processor always receives one canonical resolved output path.
- Fixed local sitemap loading and nested local sitemap resolution for directory-based processing.
- Fixed the README clone URL to the canonical Phase3Dev repository URL.
- Fixed the retry path for non-403/non-429 HTTP status codes so sleep delays remain promptly interruptible.
- Fixed the generic retry sleep path so Ctrl+C is handled promptly during backoff waits.
- Fixed `get_current_ip()` to avoid a bare `except` that could swallow `KeyboardInterrupt` or `SystemExit`.
- Fixed shared-state accounting under multithreaded runs so retry, error, sitemap, and page counters do not lose updates.
- Fixed shared failure tracking under multithreaded runs so failed sitemap state is updated consistently.
- Fixed request pacing under multithreaded runs by serializing access to the shared request clock, preserving global delay semantics.
- Fixed cross-thread races on request-scoped proxy and user-agent state by keeping them local to each request instead of storing them on the processor instance.
- Fixed a sitemap root truthiness check to use `root is None`, avoiding `ElementTree` deprecation warnings.

### Added

- Added `all_extracted_urls.txt`, always written at the end of a run with the sorted deduplicated union of all extracted page URLs.
- Added the standard metadata header to the merged output file, matching per-sitemap output headers.
- Added a minimal `SECURITY.md`.
- Added stdlib-based regression tests covering interruptible sleep handling, proxy IP formatting and interrupt propagation, locked stat/failure updates, and a threaded local sitemap processing run.

### Changed

- Kept per-sitemap files as the default output behavior while making filenames collision-resistant.
- Removed unused `lxml` from `requirements.txt`.
- Documented supported Python as `3.9+` in the README.
- Aligned README and CLI help text with actual runtime behavior for `--stealth`, directory scanning, and merged output generation.
