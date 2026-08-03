# AWS Nautilus Research Architecture

## Purpose

This architecture supports three connected workflows:

1. **NautilusTrader research and backtesting** on historical Bybit order book and trade data.
2. **Machine learning and deep learning research** on order book, mempool, and derived feature datasets.
3. **Future live trading infrastructure**, when a live strategy is ready.

The design keeps permanent state in S3 and uses compute only when needed.

---

## Core Design Principle

```text
S3 = permanent data lake
ECR = permanent Docker research environment
EC2 = disposable Nautilus research/backtest machine
SageMaker = optional ML notebook/training environment
MacBook = control room / code editor / launch client
```

Do not keep expensive compute running continuously until there is a live strategy.

---

## High-Level Architecture

```text
MacBook
  ├── GitHub repo
  ├── AWS CLI launch scripts
  ├── VS Code Remote SSH
  └── local notebook prototyping on small samples

AWS
  ├── S3 bucket
  │     ├── raw/
  │     ├── staging/
  │     ├── nautilus_catalog/
  │     ├── features/
  │     ├── labels/
  │     ├── models/
  │     └── manifests/
  │
  ├── ECR repository
  │     └── nautilus-research Docker image
  │
  ├── EC2 research instance, launched only when needed
  │     ├── pulls Docker image from ECR
  │     ├── pulls selected data from S3
  │     ├── runs Nautilus ETL/backtests/feature extraction
  │     └── syncs results back to S3
  │
  ├── optional SageMaker notebook
  │     ├── reads feature datasets from S3
  │     ├── runs ML/deep learning workflows
  │     └── writes models/results back to S3
  │
  └── future live Nautilus node
        ├── not required yet
        ├── later runs on small always-on EC2/VPS
        └── persists live stream/logs to S3
```

---

