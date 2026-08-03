# Data Loading Pipeline Structure

This repository structure is designed for a robust, modular machine learning and quantitative trading data pipeline. It separates concerns into data ingestion, label generation, feature engineering, and final dataset compilation, making it well-suited for processing high-volume financial time series.

## Directory Tree

```text
dataLoadingPipeline/
├── ingestion/
│   ├── bar_loader.py
│   ├── tick_loader.py
│   ├── orderbook_loader.py
│   └── funding_loader.py
├── labelling/
│   ├── base_labeler.py          # Abstract base class
│   ├── triple_barrier.py        # Classic ML label
│   ├── trend_scanning.py        # Trend-following label
│   ├── microstructure.py        # Order-flow labels
│   └── regime_labeler.py        # Market regime labels
├── features/
│   ├── base_feature_engineer.py
│   ├── technical.py
│   ├── micro_features.py
│   └── derived.py
├── collection/
│   ├── dataset_builder.py       # Assembles features + labels
│   ├── dataset_splits.py        # Walk-forward, time-based splits
│   └── dataset_versioner.py     # Track versions
└── pipeline.py                  # Orchestrates everything