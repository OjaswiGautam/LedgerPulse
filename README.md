# LedgerPulse

**Concept-Drift Adaptive Fraud Detection & Governance Research Prototype**

![Python 3.11](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-OpenAPI%203.1-009688?logo=fastapi&logoColor=white)
![React 18](https://img.shields.io/badge/React-18%20%2B%20TypeScript-61DAFB?logo=react&logoColor=black)
![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16%20%2B%20pgvector-4169E1?logo=postgresql&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-Parquet%20Lake-FFF000?logo=duckdb&logoColor=black)
![Status](https://img.shields.io/badge/Status-Phase%201%20In%20Progress-orange)

**LedgerPulse** is an open, reproducible research prototype for **drift-aware fraud decision support** over non-stationary payment streams. Rather than treating fraud detection as a static classification task—or blindly retraining whenever a statistical detector fires—LedgerPulse answers five core questions under strict temporal discipline:

1. **What shifted?** (Distinguishing benign covariate shift, prior shift, data-quality issues, and true concept drift)
2. **Where did it shift?** (Global vs. segment-level drift across merchant category, payment rail, device type, and geography)
3. **When is adaptation justified?** (Triage with calendar-event awareness and evidence levels $E_0, E_1, E_2$)
4. **Is the challenger actually safer?** (Pre-registered 8-check promotion gate with counterfactual replay and anti-forgetting checks)
5. **Can a human audit and approve it?** (Human-in-the-loop approval, SHAP drift attribution, and citation-validated advisory reports)

> **Scope Notice:** LedgerPulse is designed as a **drift-aware decision-support research prototype**, not an autonomous production fraud-blocking engine.

---

## Core Architectural Invariants

Every module enforces seven non-negotiable invariants verified by the automated contract test suite (`backend/tests/contract/`):

| ID | Invariant | Enforcement Mechanism |
| :--- | :--- | :--- |
| **INV-1** | **Temporal Integrity** — Every data read carries an `as_of` bound. Labels are visible only if `label_available_at <= as_of`. Random splits are forbidden. | `TemporalGuard` raises hard failure `E_TEMPORAL_LEAK` |
| **INV-2** | **Alarm $\neq$ Action** — A drift alarm triggers triage and optionally a shadow challenger; it never swaps the live serving model automatically. | Policy Orchestrator state machine |
| **INV-3** | **Pre-Registration** — Gate thresholds live in an immutable, hashed `gate_policy` created before challenger evaluation. | Gate service rejects unregistered/mutated policies |
| **INV-4** | **Human Approval** — Champion promotion requires analyst approval (`auto_approve=true` is restricted strictly to offline benchmark simulations). | Promotion API + run configuration |
| **INV-5** | **Immutability & Audit** — Model versions, scores, gate runs, and reports are write-once; every state transition appends to `audit_log`. | PostgreSQL constraints + service layer |
| **INV-6** | **Contract-First** — Frontend and backend interact exclusively through `contracts/openapi.yaml` and `contracts/events.schema.json`. | CI contract tests (`Schemathesis` + OpenAPI lint) |
| **INV-7** | **Reproducibility** — Every run persists its random seed, `config_hash`, `dataset_hash`, `git_sha`, and dependency lock hash. | Run service + MLflow lineage |

---

## System Architecture

LedgerPulse is built as a **modular monolith** deployed on a single host via Docker Compose—avoiding Kafka/Kubernetes overhead while preserving clean service boundaries. Identical domain code runs in **Simulation Mode** (deterministic virtual clock) and **Interactive Mode** (live wall clock) by swapping only the `Clock` and `EventSource` interfaces.

```text
+-----------------------------------------------------------------------------+
|                    Frontend SPA (React 18 + TypeScript)                     |
|  Run Console | Live Monitor | Drift Explorer | Alarm Inbox | Gate Results   |
|  Model Registry | Case Queue | Report Viewer | Copilot | Benchmarks         |
+--------------------------------------+--------------------------------------+
                                       | REST (/api/v1) & SSE (Last-Event-ID)
                                       v
+-----------------------------------------------------------------------------+
|               API Process (FastAPI + Pydantic v2 + RBAC Auth)               |
+--------------------------------------+--------------------------------------+
                                       |
                                       v
+-----------------------------------------------------------------------------+
|     PostgreSQL 16 (+ pgvector): Control Plane, SKIP LOCKED Queue, Outbox    |
+-------+---------------------+----------------------+------------------------+
        |                     |                      |
        v                     v                      v
+---------------+   +-------------------+   +-------------------+   +---------------+
| worker-stream |   |     worker-ml     |   |   worker-report   |   | worker-bench  |
| Replay, Ingest|   | Train, Counterfac.|   | SHAP Attribution, |   | Policy Matrix |
| Features,Score|   | Replay, Gate Eval |   | RAG Report Builder|   | Runner        |
| Drift, Triage |   | Calibration       |   | Citation Validator|   |               |
+-------+-------+   +---------+---------+   +---------+---------+   +-------+-------+
        |                     |                       |                     |
        +---------------------+-----------+-----------+---------------------+
                                          |
                                          v
          +-------------------------------+-------------------------------+
          |   Parquet Lake (/data) — Queried via in-process DuckDB        |
          |   + MLflow Artifact & Experiment Tracking Store               |
          +---------------------------------------------------------------+
```

### Strict Layering (`L0` $\rightarrow$ `L5`)
Modules may only import from lower layers (enforced in CI via `import-linter`):
*   **`L0` (`core`):** `Clock`, `TemporalGuard`, config/hashing, ULIDs, structured logging, error taxonomy, shared DTOs.
*   **`L1` (`storage`):** PostgreSQL repositories (`SKIP LOCKED` job queue + outbox), atomic Parquet I/O, per-process in-memory DuckDB, MLflow client.
*   **`L2` (`datasets`, `simulator`, `replay`, `ingestion`, `features`):** Canonical adapters, Tier-3 drift & label-delay simulator, chronological replay, Pandera validation/quarantine, shared as-of velocity features.
*   **`L3` (`models`, `monitoring`, `explain`):** Champion & Challengers (`CHAMP`, `CH-A`–`CH-D`), shadow scoring, `D1`–`D7` drift detectors, SHAP `TreeExplainer`.
*   **`L4` (`governance`, `hitl`, `reporting`, `benchmark`):** Policy orchestrator, 8-check promotion gate & counterfactual replay, analyst case queue, citation-validated RAG reports, benchmark harness.
*   **`L5` (`api`, `workers`, `cli`):** FastAPI routers, SSE publisher, worker entrypoints, admin CLI.

---

## Core ML & Governance Capabilities

### 1. Tiered Datasets & Controlled Drift Simulation
*   **Tier 1 — ULB Credit Card:** Imbalance and probability-calibration baseline (hour-level windows for smoke testing).
*   **Tier 2 — IEEE-CIS Fraud Detection:** Feature-rich entity engineering and chronological splits.
*   **Tier 3 — PaySim + Custom Simulator:** Deterministic injection of **abrupt, gradual, incremental, and recurring drift** (covariate, prior, concept, and segment campaigns) with configurable **label arrival delays** and verified `drift_truth` timestamps.

### 2. Multi-Signal Drift Monitoring (`D1`–`D7`)
*   **Unsupervised (Real-Time):** `D1` Feature PSI, `D2` IQR-normalized Wasserstein-1, `D3` Champion score shift, `D7` Representation shift (MMD).
*   **Supervised (Label-Arrival Ordered):** `D4` Positive-rate prior shift, `D5` **ADWIN** on the error stream ordered by `label_available_at` ($\delta = 0.002$), `D6` Rolling PR-AUC and Recall @ alert budget.
*   **False-Alarm Control:** $k$-of-$m$ consecutive window breaches (default 3 of 4), Benjamini–Hochberg FDR control ($q = 0.05$) across segments, and `calendar_event` matching to flag benign seasonal shifts.

### 3. Champion–Challenger & Promotion Gate (`G1`–`G8`)
*   **Model Families:** `CHAMP` (long-history GBDT), `CH-A` (sliding-window GBDT), `CH-B` (warm-start recency-weighted GBDT), `CH-C` (River incremental learner), and `CH-D` (Platt/isotonic recalibration).
*   **Anti-Forgetting:** Stratified historical positive replay buffer + 15% reserved **anchor holdout** excluded from all challenger training.
*   **8-Check Promotion Gate:** Evaluates challengers via counterfactual replay on frozen post-cutoff feature snapshots with block-bootstrap confidence intervals:
    *   `G1` Label Maturity ($\ge 80\%$, fail $\rightarrow$ `INCONCLUSIVE`), `G2` PR-AUC gain, `G3` Recall @ alert budget, `G4` Calibration (ECE & Brier), `G5` Expected net financial cost, `G6` Anti-forgetting on anchor holdout ($\le 0.02$ drop), `G7` Segment non-regression, `G8` Alert volume stability ($\pm 25\%$ soft warning).

### 4. HITL, Explainability & Advisory RAG
*   **Analyst Case Queue:** Top alert-budget transactions surface for manual review; analyst labels immediately enter the label stream and take precedence over later chargebacks.
*   **SHAP Drift Attribution:** Explains both individual alerts (top-5 features) and window-to-window feature importance shifts.
*   **Citation-Validated Reports & Copilot:** Deterministic evidence JSON paired with an optional `pgvector` RAG narrative. Any claim lacking a valid `chunk_id` or `evidence_id` is stripped; LLM outages never block scoring, alarms, or gates.

---

## Tech Stack

| Layer | Technologies |
| :--- | :--- |
| **Backend & API** | Python 3.11, FastAPI, Pydantic v2, Pandera, Alembic |
| **Control Plane & Queue** | PostgreSQL 16 (`pgvector`, `SKIP LOCKED`, `LISTEN/NOTIFY`) |
| **Analytic Lake** | Apache Parquet + DuckDB (in-process) + Polars / Pandas |
| **ML, Drift & Explainability** | LightGBM, XGBoost, River (ADWIN/online), SciPy, Evidently, SHAP, MLflow |
| **Frontend** | React 18, TypeScript, Vite, TanStack Query, ECharts |
| **Contracts & QA** | OpenAPI 3.1, `openapi-typescript`, Prism (mock), Schemathesis, `pytest`, `import-linter`, `ruff`, `mypy` |

---

## Repository Structure

```text
ledgerpulse/
├── .github/workflows/       # CI: backend lint/types/layers, contracts, frontend
├── docs/
│   ├── architecture/        # System Architecture Spec & ADRs
│   ├── research/            # Feasibility report & literature notes
│   ├── runbooks/            # Setup & operational guides
│   └── daily-progress-reports/  # Daily team logs (YYYY-MM-DD.md)
├── contracts/               # [T0] openapi.yaml, events.schema.json, Prism fixtures
├── config/                  # Versioned, hashed YAML policies & drift scenarios
├── deploy/                  # Single-host Docker Compose, Dockerfiles, nginx.conf
├── backend/
│   ├── ledgerpulse/         # Modular monolith (L0 core -> L5 api/workers/cli)
│   ├── migrations/          # Alembic PostgreSQL migrations
│   └── tests/               # unit/, integration/, contract/ (INV-1..INV-7)
├── frontend/                # React 18 + TS SPA (9 domain feature modules)
├── data/                    # Gitignored local volume (/data in Docker)
└── scripts/                 # Data download & DB seeding utilities
```

---

## Quick Start

### Prerequisites
*   Docker & Docker Compose v2+
*   Python 3.11+ (`uv` or `pip`)
*   Node.js 20+ (for local frontend development)

### 1. Run Full Stack via Docker Compose
```bash
docker compose -f deploy/docker-compose.yml up --build
```
*   **Frontend SPA:** `http://localhost:3000`
*   **FastAPI Docs (OpenAPI):** `http://localhost:8000/docs`
*   **MLflow Tracking UI:** `http://localhost:5000`

### 2. Local Backend Development & Quality Checks
```bash
cd backend
pip install -e ".[dev]"

# Run linter, type checker, and L0-L5 architecture boundary check
ruff check .
mypy ledgerpulse
lint-imports

# Run unit, integration, and invariant contract tests
pytest tests/
```

### 3. Local Frontend Development (Contract-First with Prism Mock)
Frontend development runs independently of backend completion using the OpenAPI contract:
```bash
cd frontend
npm install

# Generate TypeScript API types from contracts/openapi.yaml
npm run generate:api

# Start Vite dev server
npm run dev
```

---

## Evaluation Policy

All benchmark comparisons (`static` vs. `periodic` vs. `drift_triggered` vs. `selective_incremental`) run strictly under **rolling-origin / prequential evaluation** across `policy` $\times$ `dataset` $\times$ `scenario` $\times$ `seed`:
*   **Core Metrics:** Rolling PR-AUC, Recall @ fixed alert budget, ECE & Brier score, and cumulative net financial cost.
*   **Tier-3 Drift Metrics:** Detection delay, false-alarm rate, recovery time (time to return to $\ge 95\%$ of pre-drift recall @ budget), retrain count, and rejected promotion count.

---

## Roadmap & Team Ownership

### Phased Delivery
*   **Phase 1 — Reproducible Benchmark (`T0`–`T3` + baseline `T7`):** OpenAPI/SSE contracts, `TemporalGuard`, Postgres + Parquet/DuckDB storage, ULB/IEEE-CIS/PaySim adapters, as-of feature engine, static/periodic GBDT baselines.
*   **Phase 2 — Drift & Governance Engine (`T4`–`T5`):** Global & segment-level `D1`–`D7` detectors, triage orchestrator, challenger training (`CH-A`–`CH-D`), 8-check promotion gate, and counterfactual replay.
*   **Phase 3 — Decision Support (`T6`):** SHAP drift attribution, HITL analyst case queue, citation-validated RAG reports, and Copilot Q&A.

### Team Allocation
| Role | Primary Ownership |
| :--- | :--- |
| **Frontend Engineer** | `frontend/` (9 React views, ECharts, Prism mocks, SSE client) |
| **Backend Engineer** | `L0`–`L1` core/storage, `C1`/`C4` adapters & ingestion, `C3` replay, `C9` orchestrator, `C13` HITL, FastAPI & workers |
| **ML Engineer** | `C2` Tier-3 simulator, `C5` features, `C6`–`C7` models/scoring, `C8` drift math, `C10` gate math, `C11`–`C12` SHAP/RAG, `C14` benchmarks |
| **QA Engineer** | `backend/tests/contract/` (`INV-1`–`INV-7`), integration tests, Schemathesis conformance, E2E verification |
| **Security, DevOps & Code Reviewer** | `contracts/` (`T0` gatekeeper), `deploy/` (Docker Compose), `.github/workflows/` (CI/CD), `config/` governance, RBAC & audit review |

Daily progress is tracked per team member in [`docs/daily-progress-reports/`](docs/daily-progress-reports/).

---

## Documentation
*   [System Architecture & Technical Specification](docs/architecture/)
*   [Research Feasibility & Literature Review](docs/research/)
*   [Daily Progress Reports](docs/daily-progress-reports/)
*   [OpenAPI 3.1 Contract](contracts/openapi.yaml) | [SSE Event Schemas](contracts/events.schema.json)