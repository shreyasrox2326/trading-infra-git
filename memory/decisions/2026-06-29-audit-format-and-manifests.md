# Audit Format And Manifest Decisions

Date: 2026-06-29

Decisions:

- Bhavcopy format periods are explicit in `src/trading_infra/data/formats.yaml`.
- Date-based fetch planning uses the format registry.
- Parser validation uses filename shape to choose the parser contract, so synthetic test fixtures and real downloaded files are validated against the file format they claim to be.
- Raw fetch state is stored in `data/import/manifests/raw_fetch_<EXCHANGE>.parquet`.
- Partition build state is stored in `data/import/manifests/partition_manifest.parquet`.
- Historical upload requires a passing audit, a clean raw fetch manifest, and a readable partition manifest.

Rationale:

- The audit called out fallback URLs and alias-tolerant parsers as insufficient for operator safety.
- Manifested state makes repair, doctor, upload gating, and sync checks explicit instead of log-dependent.
