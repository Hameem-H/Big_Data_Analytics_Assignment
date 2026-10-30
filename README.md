# EpiScope — Community Epidemic Surveillance and Early-Warning Platform

## 1. Run the project after cloning

The easiest way to run EpiScope is with Docker Compose. You do not need to install Python, Java, or Apache Spark separately on your computer.

### Prerequisites

- Git
- Docker Engine
- Docker Compose (`docker compose`)
- A web browser
- Internet access for the initial Docker image and Python dependency downloads

### Clone the repository

```bash
git clone https://github.com/Hameem-H/Big_Data_Analytics_Assignment.git
cd Big_Data_Analytics_Assignment
```

### Build and start EpiScope

```bash
docker compose up --build
```

The first run builds the Docker image and may take several minutes. After the image is built, the container runs the demo workflow:

1. Generates a reproducible synthetic dataset of 100,000 encounters.
2. Runs the Bronze-to-Gold data pipeline.
3. Builds weekly district-level analytics input.
4. Runs outbreak detection and evaluates alerts against generated outbreak ground truth.
5. Generates district-level forecasts.
6. Estimates resource requirements and produces a budget allocation.
7. Exports dashboard data as JSON.
8. Starts the local web server.

When the terminal displays that the dashboard server is serving on port `8000`, open:

**http://localhost:8000/dashboard/**

Keep the terminal and container running while using the dashboard. Press `Ctrl+C` in the terminal to stop it.

### Stop the container

```bash
docker compose down
```

### Run again

```bash
docker compose up
```

Use `docker compose up --build` after changing the Docker configuration or application dependencies.

### Check the container

```bash
docker compose ps
```

The pipeline runs before the dashboard server starts. If a pipeline stage fails, the container exits and the dashboard will not start. Check the terminal output for the failed stage.

---

## 2. Project overview

EpiScope is a student-developed data engineering and analytics project for community-level epidemic surveillance. It explores how health-facility encounter records can be processed into aggregated information that may help identify unusual disease activity, understand trends, estimate near-term demand, and support resource planning.

The project uses a synthetic Bangladesh-oriented dataset. It is an academic prototype for demonstrating a data pipeline and analytics workflow; it is not a production public-health surveillance system, and its outputs should not be used as clinical or official public-health advice.

### Project objectives

- Build a repeatable data pipeline for ingesting and validating encounter records.
- Separate raw, validated, and aggregated data into processing stages.
- Identify potential outbreak signals from weekly district-level disease counts.
- Evaluate detection results against synthetic outbreak ground truth.
- Produce short-term district-level forecasts.
- Estimate resource requirements and distribute a planning budget.
- Present aggregated results through an interactive dashboard.
- Demonstrate reproducible local deployment using Docker Compose.

## 3. System architecture

### Data generation

Synthetic encounter records, facility reference data, and outbreak ground truth are used to create a repeatable demonstration dataset. The Docker demo generates 100,000 encounter records using a fixed seed.

### Data engineering pipeline

The Bronze-to-Gold pipeline processes encounter data through these stages:

- **Bronze:** ingests source encounter records and stores the ingested data in Parquet format.
- **Silver:** validates records against the data contract and reference registries. Invalid records are separated from accepted records for quality control.
- **Gold:** creates aggregated surveillance and facility-reporting outputs for downstream use.

The orchestrator records batch processing status in a manifest and supports batch-level tracking and controlled reprocessing.

### Analytics

The analytics layer uses Apache Spark to prepare and process aggregated data.

- **Weekly aggregation:** groups validated records by week, district, and disease.
- **Outbreak detection:** compares weekly observations with the preceding three observations and calculates a C2-style score. The Docker demonstration uses a threshold of `4.0`.
- **Detection evaluation:** compares generated alerts with synthetic outbreak intervals and reports evaluation metrics.
- **Forecasting:** produces district-level forecasts for the next four weeks using the implemented time-series approach and writes backtesting metrics.
- **Resource planning:** estimates beds, IV saline units, and test kits from forecast demand, then allocates a configurable planning budget across districts.

### Dashboard and serving

The browser-based dashboard uses D3.js. It reads aggregated JSON exports and district boundary data, and presents weekly incidence, district-level information, outbreak alerts, forecasts, and resource allocation.

Encounter-level records are not exported to the dashboard. The serving export applies primary suppression to small count values before publication.

## 4. Technology stack

| Area | Technologies |
|---|---|
| Data generation and processing | Python |
| Distributed data processing | Apache Spark / PySpark |
| Storage formats | CSV, Parquet, JSON |
| Data validation | Python validation logic and JSON Schema |
| Analytics | PySpark, Spark SQL and window operations |
| Dashboard | HTML, JavaScript, D3.js |
| Geographical visualization | Bangladesh district GeoJSON |
| Containerization | Docker, Docker Compose |
| Version control | Git and GitHub |

## 5. Repository structure

```text
.
├── analytics/
│   ├── detection/       # Weekly aggregation, outbreak detection and evaluation
│   ├── forecast/        # District-level forecasting and backtesting
│   └── optimise/        # Resource demand estimation and budget allocation
├── dashboard/
│   ├── data/            # District boundaries and district-name aliases
│   └── index.html       # Interactive dashboard
├── data/
│   ├── reference/       # District and facility reference data
│   ├── synthetic/       # Generated demonstration input (not committed)
│   └── processed/       # Pipeline and analytics outputs (not committed)
├── docker/
│   └── entrypoint.sh    # Runs the demo workflow and starts the web server
├── docs/                # Data contracts, design and analytics documentation
├── pipeline/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── orchestrator.py
├── schemas/             # Data contract schemas
├── scripts/             # Synthetic data generation scripts
├── serving/
│   ├── exports/         # Dashboard JSON exports (generated, not committed)
│   └── export_dashboard_data.py
├── tests/               # Automated tests
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## 6. Data and privacy considerations

The demonstration data is synthetic and does not represent actual patient records. The pipeline uses hashed patient identifiers in generated data, and the dashboard consumes aggregated outputs rather than encounter-level records.

Small counts are subject to primary suppression in the dashboard export. This is a prototype implementation; suppression in one output does not, by itself, establish that all possible cross-table or complementary-disclosure risks have been eliminated. The project should not be treated as a certified privacy-preserving system.

## 7. Important limitations

- The demonstration uses synthetic records and generated outbreak ground truth, not live health-facility submissions.
- Outbreak detection is a statistical prototype. Its alerts require validation against real surveillance data and review by public-health experts.
- Forecasts are experimental estimates, and their accuracy depends on the generated data and implemented assumptions.
- Resource quantities and unit costs are illustrative planning assumptions, not official clinical guidelines or procurement prices.
- The resource allocation method uses equal district weights as a baseline because the available district reference data does not provide an official population field. The resulting allocation should not be interpreted as an optimized real-world distribution.
- The dashboard is intended for demonstration and exploration, not operational decision-making.

## 8. Documentation and reproducibility

Additional technical documentation is available in:

- `docs/proposal.md` — project proposal and context.
- `docs/data_contract.md` — encounter data contract.
- `docs/data_sources.md` and `docs/reference_data_sources.md` — data-source documentation.
- `docs/encounter_generator.md` — synthetic encounter generation.
- `docs/analytics/outbreak_detection.md` — outbreak detection and evaluation workflow.
- `docs/analytics/visualisation_handover.md` — analytics outputs for dashboard integration.
- `docs/gold/` — Gold-layer output documentation.
- `AI_USAGE.md` — disclosure of AI-assisted work.

## 9. Team and submission information

**Course:** Big Data Analytics  
**Project:** EpiScope  
**Repository:** https://github.com/Hameem-H/Big_Data_Analytics_Assignment

**Team members and contributions:** Add the final agreed member names, student IDs, and contribution percentages here. Ensure the percentages match the signed contribution matrix submitted with the report.

**Project demonstration video:** Add the final accessible video link here.

---

This project was developed as an academic team assignment to apply data engineering, distributed processing, analytics, and visualization concepts to a synthetic epidemic-surveillance use case.
