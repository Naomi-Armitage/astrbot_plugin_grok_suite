# Changelog

## Unreleased - 2026-03-25

### Added

- Added regression coverage for image response fallback handling, prompt normalization, and multi-result extraction.
- Added a short "Recent Updates" section in the README to summarize the current main-branch fixes.

### Changed

- Synced `main` with the full change set from `codex/update-naomi-sync-20260323`.
- Normalized image prompts so text-to-image requests default to `Generate an AI image of:` and image-to-image requests default to `Edit this image to:` when a compatible prefix is missing.

### Fixed

- Fell back to non-stream image extraction when streamed Grok image responses do not contain media.
- Preserved multiple image results when Grok returns more than one image URL in a single response payload.
