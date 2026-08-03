# Quantitative ML & Execution Roadmap

This roadmap outlines the progressive development phases for integrating advanced machine learning features, state-space filters, and execution logic within the Nautilus Trader framework. The core architectural philosophy is the strict decoupling of heavy feature computation (external) from deterministic, low-latency execution (internal).

---

## Phase Overview

| Phase | Focus | Core Architecture | Key Milestones |
| :--- | :--- | :--- | :--- |
| **1** | **Offline Feature Cataloging** | `ParquetDataCatalog` & Vectorized Pipelines | Ingest raw data, compute GARCH/HMM, save as `CustomData`. |
| **2** | **Live Feature Ingress (ZMQ)** | Dual-Process (ZeroMQ IPC/UDP) | Stream real-time features from an external process into Nautilus. |
| **3** | **Meta-Labeling & Boundaries** | Triple-Barrier Method & Meta-Classifiers | Build labeling pipelines, train "bouncer" models on simple rules. |
| **4** | **State-Space Signal Filters** | Kalman (Internal) & Particle Filters (External) | Integrate hidden-state tracking for volatility and price regimes. |
| **5** | **Advanced Production ML** | End-to-End ML Execution | Deploy deep learning or ensemble models live. |

---

## Phase Details

### Phase 1: Offline Feature Cataloging & Cold Storage
* **Objective:** Establish the foundation for historical feature research without clogging the backtesting engine.
* **Execution:**
    * Query historical data from the Nautilus `ParquetDataCatalog`.
    * Run heavy batch computations offline (Log Returns, Fractional Differentiation, GARCH, HMM regimes).
    * Persist these engineered features back into the catalog as `CustomData`.

### Phase 2: Live Feature Ingress (The ZMQ Message Bus)
* **Objective:** Build a low-latency pipeline to stream heavy, live-calculated features into the execution loop.
* **Execution:**
    * **External Process (The Brain):** Calculates live feature arrays sequentially as market data ticks arrive.
    * **ZMQ Bridge:** Publishes serialized payloads over IPC or local UDP.
    * **Nautilus Client (The Receiver):** A custom Data Client listens to the socket, translates payloads into native `CustomData`, and dispatches them to the internal message bus.

### Phase 3: Meta-Labeling & Path Segmentation
* **Objective:** Develop the core labeling pipeline using simple trading rules to prove the framework before using advanced ML.
* **Execution:**
    * Extract forward path snapshots (`forward_path_array`) alongside features.
    * Apply the **Triple-Barrier Method** (volatility-adjusted take-profit, stop-loss, and time-expiry) to generate directional labels.
    * Build a secondary **Meta-Classifier** (binary model) to predict the probability of primary strategy success, filtering out false positive signals.

### Phase 4: State-Space Filtering (Kalman & Particle)
* **Objective:** Implement noise-reduction and tracking algorithms to estimate the true state of volatile markets.
* **Execution:**
    * **Kalman Filter:** Implement as a native, lightweight Nautilus `Indicator` for $O(1)$ real-time price or parameter smoothing.
    * **Particle Filter:** Implement in the external Python process due to high Monte Carlo computational requirements, feeding state estimates back via the ZMQ pipeline.

### Phase 5: Advanced Production ML
* **Objective:** Scale the pipeline to handle sophisticated, non-linear machine learning architectures.
* **Execution:**
    * Train complex classifiers (e.g., XGBoost, LSTM, Transformers) using the validated data-leakage-free cross-validation framework.
    * Execute model inference live, utilizing the Phase 2 architecture for sub-millisecond execution routing.