# Changelog

All notable changes to this project are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.0.0] – 2024-01-01

### Added
- `DLISBuilder` fluent programmatic API
- `LASConverter` for LAS 1.2 / 2.0 / 3.0 input
- `CSVConverter` with auto-detection of depth column and unit rows
- `DataSource` protocol for database / API integration (zero-coupling)
- Multi-frame support: channels with different depth samplings written to one DLIS file
- String / categorical channel encoding as `int32` codes with `LABELS_<name>` Parameter
- Array / image channel support with `Resolution.LOW` (float32) and `Resolution.HIGH` (float64)
- `FrameDef` dataclass for manual frame grouping
- `detect_csv_layout()` helper for previewing auto-detection results
- Batch conversion with `convert_batch()` on both converters
- CLI entry point (`dlis-builder`)
- `py.typed` marker — fully typed library
