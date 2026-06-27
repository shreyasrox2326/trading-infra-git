# Tech Stack

Date: 2026-06-27

## Decisions

- Python-first codebase.
- No Rust initially.
- Use Polars and DuckDB for Parquet scans and transformations.
- Use NumPy for arrays.
- Use PyTorch, sklearn, XGBoost, and LightGBM for modelling.
- Use GitHub Actions for scheduled daily processing.
- Use Cloudflare R2 for object storage.
- Use the local machine for research, full backtesting, training, and experiments.

## Local Hardware

```text
6-core CPU
RTX 3050 GPU
```

## Online Assumption

GitHub Actions daily inference should be treated as CPU-only.

Deployed models should be compact and CPU-friendly.

Training and experiments can use the local GPU.
