# MagicBricks Real Estate Lakehouse — Multi-Source Medallion Pipeline on Databricks

![Databricks](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-Databricks-0078D4)
![Unity Catalog](https://img.shields.io/badge/Unity-Catalog-blue)
![Lakeflow](https://img.shields.io/badge/Lakeflow-Declarative%20Pipelines-green)
![DAB](https://img.shields.io/badge/Deployment-Databricks%20Asset%20Bundles-orange)

> An end-to-end lakehouse built on Azure Databricks that turns ~18,000 scraped real-estate listings from **six Indian cities** into eight analytics-ready Gold marts — ingested through **six different cloud channels**, streamed through a single Lakeflow Declarative Pipeline, governed by Unity Catalog, and deployed entirely as code with Databricks Asset Bundles.

This isn't a toy ETL notebook. It's a working simulation of what a real data platform team ships: heterogeneous ingestion, a governed Medallion architecture, data-quality enforcement with quarantine, and one-command infrastructure deployment — no manual UI clicks anywhere in the pipeline.

---

## 📚 Table of Contents

- [Project at a Glance](#-project-at-a-glance)
- [Why This Project](#why-this-project)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Data Sources — Six Ingestion Channels, One Pipeline](#data-sources--six-ingestion-channels-one-pipeline)
- [Project Structure](#project-structure)
- [Medallion Layers](#medallion-layers)
- [Deployment as Code — Databricks Asset Bundles](#deployment-as-code--databricks-asset-bundles)
- [Implementation Guide](#implementation-guide)
- [Local Scrapers](#local-scrapers-mainpy--rest_apipy)
- [Security Notes](#security-notes)
- [Dashboard](#️-dashboard)
- [Results](#-results)
- [Skills Demonstrated](#-skills-demonstrated)
- [Future Improvements](#future-improvements)
- [Author](#author)
- [License](#-license)

---

## 📊 Project at a Glance

| Metric | Value |
|---|---|
| Cities covered | 6 |
| Ingestion channels | 6 |
| Property listings processed | ~18,000 |
| Bronze tables | 2 |
| Silver tables | 3 |
| Gold materialized views | 8 |
| Databricks Jobs | 1 (multi-task DAG) |
| Lakeflow pipelines | 1 |
| Deployment method | Databricks Asset Bundles |

---

## Why This Project

Real-world data engineering rarely gets to assume one clean source system. This project was deliberately built to simulate that reality: the same six Indian cities of MagicBricks listings arrive through **six genuinely different ingestion paths** — local upload, a custom REST API push, GitHub, Google Drive, Azure Blob Storage, and AWS S3 — and still have to land in one unified, governed table.

That constraint is the point. It forces the kind of engineering decisions that show up in production platforms: schema-on-read tolerance, source-agnostic streaming ingestion, secrets management across providers, and a deployment model where the whole system — catalog, volumes, pipeline, job — is defined once as code and stood up with a single command.

**Highlights:**

- Databricks Asset Bundles (DAB) for config-as-code deployment across environments
- Lakeflow Declarative Pipelines — streaming tables and materialized views, no manual orchestration logic
- Unity Catalog governance from catalog down to volume and table level
- Auto Loader–based streaming ingestion with schema evolution built in
- Databricks Secrets for cross-provider credential management (no hardcoded keys)
- An explicit multi-task Job DAG: setup → ingest → transform
- Eight business-ready Gold materialized views, feeding a full AI/BI dashboard suite

---

## Architecture

```text
GitHub / Google Drive / Azure Blob / AWS S3 / REST API / Local Upload
                                │
                                ▼
                     Databricks Volumes (raw)
                                │
                                ▼
                 Lakeflow Declarative Pipeline
                                │
   ┌───────────────────────────────────────────────────────────┐
   │ Bronze (raw JSON, Auto Loader)                             │
   │   → Bronze Flattened (one row per listing)                 │
   │   → Silver Cleaned (normalization, typing, parsing)        │
   │   → Silver Validated / Silver Quarantine (business rules)  │
   │   → Gold Materialized Views (dashboard-ready marts)        │
   └───────────────────────────────────────────────────────────┘
                                │
                                ▼
                  Databricks AI/BI Dashboards
```

Six independent entry points converge into one streaming Bronze table, then flow through a single, linear Medallion pipeline into eight purpose-built Gold marts — the fan-in/fan-out shape that a multi-source platform actually needs.

---

## Tech Stack

| Category | Technology |
|---|---|
| Cloud | Azure Databricks (Unity Catalog–enabled) |
| Language | Python, PySpark, SQL |
| Governance | Unity Catalog |
| Storage | Databricks Volumes |
| Processing | Lakeflow Declarative Pipelines |
| Streaming | Auto Loader |
| Deployment | Databricks Asset Bundles (DAB) |
| Orchestration | Databricks Jobs (multi-task DAG) |
| Secrets | Databricks Secrets |
| Version control | Git & GitHub |
| Visualization | Databricks AI/BI Dashboards |

---

## Data Sources — Six Ingestion Channels, One Pipeline

Every listing is scraped from a **local machine** — there's no cloud-side scraping step anywhere in this project. What differs per city is the *route* that locally scraped JSON takes before Auto Loader ever sees it, which is where most of the ingestion engineering actually lives.

| City | Channel | How the data gets there |
|---|---|---|
| Ahmedabad | Local upload | Scraped locally, dropped straight into the `local_upload_amd` Volume — no intermediate hop |
| Delhi NCR | REST API | Scraped locally by `rest_api.py`, saved to disk **and** pushed directly to the `rest_api_delhi_ncr` Volume via the Files API in the same run |
| Chennai | GitHub | Scraped locally, pushed to a GitHub repo, pulled back down by `01_Data_Ingestion.py` via the GitHub Contents API |
| Mumbai | Google Drive | Scraped locally, uploaded to a Drive folder, pulled by `01_Data_Ingestion.py` via the Drive API (key stored in Databricks Secrets) |
| Bangalore | Azure Blob Storage | Scraped locally, pushed to ADLS Gen2, copied into the Volume with `dbutils.fs.cp` |
| Hyderabad | AWS S3 | Scraped locally, pushed to S3, read directly by Auto Loader |

Ahmedabad is the only city with zero dispersal — it's the pure baseline case. Delhi NCR is the only one where the scraper pushes straight into a Databricks Volume without any intermediate cloud store in between. GitHub, Google Drive, Azure Blob, and S3 each act as a genuine third-party "drop point," which is what makes this feel like integrating with real external systems rather than one convenient upload path.

The Ahmedabad and Delhi NCR scrapers (`Main.py` / `rest_api.py`) run **locally**, outside Databricks — see [Local Scrapers](#local-scrapers-mainpy--rest_apipy).

---

## Project Structure

```text
magicbricks-real-estate-lakeflow-databricks/
│
├── databricks.yml                          # bundle root config
├── resources/
│   ├── mb_pipeline.yml                     # Lakeflow pipeline resource
│   └── mb_jobs.yml                         # orchestration job (setup → ingest → pipeline)
├── setup/
│   └── 00_Setup_Catalog.py                 # idempotent schema/volume setup
├── 01_Data_Ingestion.py                    # GitHub / Google Drive / Azure Blob ingestion
├── transformations/
│   ├── 02_Bronze_Ingestion.py              # Auto Loader → mb_listings_raw (6 city flows)
│   ├── 03_Bronze_Flattened.py              # explode resultList → one row per listing
│   ├── 04_Silver_Cleaning.py               # null/type/date/URL/address/description cleaning
│   ├── 05_Silver_Expectation.py            # business rules → validated / quarantine
│   └── 06_Gold_Layer_Materialized_Views.py # 8 dashboard-ready Gold marts
├── scripts/
│   ├── Main.py                             # local scraper (Ahmedabad)
│   └── rest_api.py                         # local scraper + Files API upload (Delhi NCR)
└── README.md
```

---

## Medallion Layers

### Bronze — `02_Bronze_Ingestion.py`

- One streaming table, `bronze.mb_listings_raw`, fed by six parallel `append_flow`s (one per city), each reading raw JSON text off a Volume via Auto Loader.
- One row = one scraped JSON file, tagged with source file metadata, city, source name/type, and ingestion timestamp.
- Row-level expectations (`raw_json` not empty, `city` present, file metadata present) enforced with `expect_all`.

### Bronze Flattened — `03_Bronze_Flattened.py`

- Parses the raw JSON against an explicit `StructType` schema and explodes `resultList` so that one row = one property listing.
- Keeps amenities, images, and landmark details as arrays rather than exploding them further, avoiding a cartesian blow-up across unrelated nested arrays.

### Silver Cleaning — `04_Silver_Cleaning.py`

Normalizes ~70 business fields: null standardization, string trimming/whitespace collapse, categorical casing, integer/timestamp casting, date parsing (including relative "today"/"yesterday" values and `possession`/`modified` date formats), URL cleanup, address normalization, HTML stripped from descriptions, and JSON-encoded amenity arrays parsed and de-duplicated. No rows are dropped at this stage — every cleaned Bronze record is preserved.

### Silver Validation — `05_Silver_Expectation.py`

Applies eleven business rules (valid listing ID, sane price range, plausible bedroom/bathroom counts, floor-within-building, minimum title length, valid URL, plausible lat/long, has at least one image, etc.) and splits records into:

- `silver.silver_validated` — records that pass every required rule
- `silver.silver_quarantine` — records that fail one or more, tagged with `failed_expectations` and `failed_expectation_count`

### Gold — `06_Gold_Layer_Materialized_Views.py`

`gold_listing_master_current` de-duplicates Silver down to one current record per `listing_id` (ranked by modified/ingestion/validation timestamp) and adds derived business columns — price per sqft, carpet-area price, coordinate usability, possession category, listing source flags, freshness flags, and outlier/benchmark flags. Every other Gold view reads from this table:

| View | Purpose |
|---|---|
| `gold_listing_master_current` | One current row per listing, business-enriched |
| `gold_city_summary` | City-level inventory and pricing rollups |
| `gold_locality_summary` | Locality-level inventory and pricing rollups |
| `gold_market_inventory` | Segment-level supply breakdown |
| `gold_price_benchmark` | Locality/segment price & price-per-sqft benchmarks, with premium-vs-city and dispersion metrics |
| `gold_project_summary` | Project/society-level inventory and pricing intelligence |
| `gold_freshness_summary` | New/modified/stale listing counts by city & locality |
| `gold_quality_coverage` | Field-completeness and a weighted data-quality score/band |

---

## Deployment as Code — Databricks Asset Bundles

The project deploys as a single DAB: `databricks.yml` includes the two resource files under `resources/`, and exposes one variable — `catalog` — that every notebook reads via `spark.conf.get("catalog", ...)` (or a notebook widget for `00_Setup_Catalog.py` / `01_Data_Ingestion.py`), so the same bundle can target a different catalog per environment without editing a single notebook.

**`resources/mb_pipeline.yml`** — declares the Lakeflow pipeline and its five transformation notebooks (Bronze through Gold), parameterized by `${var.catalog}`.

**`resources/mb_jobs.yml`** — declares `mb_end_to_end`, a manually triggered job with three tasks that run in order:

```text
setup_catalog  →  ingest_external_sources  →  refresh_medallion_pipeline
```

`setup_catalog` and `ingest_external_sources` run as notebook tasks; `refresh_medallion_pipeline` triggers the Lakeflow pipeline itself.

### What isn't in the bundle

`scripts/Main.py` and `scripts/rest_api.py` scrape MagicBricks directly from a laptop, not inside Databricks — there's nothing for a Databricks bundle to deploy there. They're run manually (or via your own scheduler) **before** the ingestion job, so the Ahmedabad/Delhi NCR volumes have data for Auto Loader to pick up.

---

## Implementation Guide

### 1. Install the Databricks CLI

```bash
winget install Databricks.DatabricksCLI
databricks -v      # need 0.283.0+
```

### 2. Authenticate

```bash
databricks auth login --host https://<your-workspace-url>.azuredatabricks.net
```

Accept the default profile name, or give it one (e.g. `mb-dev`). Confirm it saved correctly:

```bash
databricks auth profiles
```

### 3. Create the Unity Catalog catalog

Catalog creation is account/metastore-level, so it's intentionally left outside the bundle as a one-time manual step:

```sql
CREATE CATALOG IF NOT EXISTS adb_real_estate_mb;
```

### 4. Create the Google Drive secret

`01_Data_Ingestion.py` pulls the Google Drive API key from Databricks Secrets rather than hardcoding it:

```bash
databricks secrets create-scope mb_real_estate
databricks secrets put-secret mb_real_estate gdrive_api_key
```

### 5. Configure `databricks.yml`

Set `targets.dev.workspace.host` to your workspace URL and confirm the default `catalog` variable matches the catalog you created in step 3.

### 6. Validate the bundle

```bash
databricks bundle validate
```

### 7. Deploy

```bash
databricks bundle deploy --target dev
```

This uploads the notebooks to your workspace and creates the pipeline and job. Dev-target deploys are paused by default — nothing is auto-scheduled.

### 8. Seed the local-upload / REST API sources

Before running the job, make sure raw JSON is actually sitting in the volumes it expects:

```bash
python scripts/Main.py        # Ahmedabad → local_upload_amd volume
python scripts/rest_api.py    # Delhi NCR → uploads via Files API
```

GitHub, Google Drive, Azure Blob, and AWS S3 are pulled live by the job itself, so they don't need a manual seeding step.

### 9. Run the end-to-end job

```bash
databricks bundle run mb_end_to_end --target dev
```

Runs `setup_catalog` → `ingest_external_sources` → `refresh_medallion_pipeline`. The first run takes longer since Bronze streaming tables and Gold materialized views are being created from scratch.

### 10. Iterate

Any time a notebook or resource YAML changes:

```bash
databricks bundle validate
databricks bundle deploy --target dev
```

### 11. Tear down (optional, for cost control)

```bash
databricks bundle destroy --target dev
```

Deletes the pipeline, job, and any tables/views it manages.

### Adding a prod target

Add a `prod:` block under `targets` in `databricks.yml` with its own `workspace.host` and `variables.catalog` — the rest of the bundle (pipeline, job, notebooks) is already environment-parameterized and needs no changes.

---

## Local Scrapers: `Main.py` / `rest_api.py`

Both scripts scrape MagicBricks search-result pages via `curl_cffi` (Chrome impersonation) through a residential proxy, retry failed pages up to three times, and log any page that never succeeds to `failed_pages.txt`.

- **`Main.py` (Ahmedabad)** saves each scraped page as JSON locally only. That local output is then dropped into the `local_upload_amd` Volume as a manual step — it's the one city with no cloud dispersal in between.
- **`rest_api.py` (Delhi NCR)** saves each scraped page as JSON locally *and*, in the same run, uploads it straight to the `rest_api_delhi_ncr` Databricks Volume via the Files REST API — so the local copy and the Volume copy exist side by side with no separate cloud storage hop.

Proxy credentials and the Databricks PAT are left blank in the script on purpose — set them as environment variables or a local `.env` file rather than committing them.

---

## Security Notes

- The Google Drive API key lives in Databricks Secrets (`mb_real_estate.gdrive_api_key`), never in a notebook.
- The Databricks PAT and proxy credentials used by the local scrapers should be kept out of source control — set them via environment variables rather than editing them into `Main.py` / `rest_api.py` directly.
- `.gitignore` should exclude any local `.env`, credentials file, or `databricks.yml` override containing a real workspace host if that host is considered sensitive.

---

## 🖼️ Dashboard

> Databricks AI/BI dashboards, built directly on top of the Gold layer.

**1. Listing Explorer**

<img width="1089" height="399" alt="le-6" src="https://github.com/user-attachments/assets/e5973dff-9be5-4003-b73a-634538412e92" />
<img width="1118" height="489" alt="le-1" src="https://github.com/user-attachments/assets/b2a0e4c0-5903-4dee-9f72-61de7080d1a4" />
<img width="1123" height="304" alt="le-2" src="https://github.com/user-attachments/assets/a6061568-75a6-4bfa-a190-81b06ed186f9" />
<img width="1115" height="409" alt="le-3" src="https://github.com/user-attachments/assets/7489ff36-b877-4514-8a0b-f04eaccd9602" />
<img width="1113" height="353" alt="le-4" src="https://github.com/user-attachments/assets/8bfe6c2f-3d68-470b-91a3-19a9c7de6053" />
<img width="1110" height="353" alt="le-5" src="https://github.com/user-attachments/assets/94fffbac-abe8-447c-9d2f-30963455cb02" />


**2. Executive Summary**

![1](Screenshots/es-6.png)
![2](Screenshots/es-5.png)
![3](Screenshots/es-3.png)
![4](Screenshots/es-2.png)
![5](Screenshots/es-1.png)

**3. City & Locality Summary**

![1](Screenshots/cl-1.png)
![2](Screenshots/cl-3.png)
![3](Screenshots/cl-2.png)

**4. Inventory Summary**

![1](Screenshots/i-1.png)
![2](Screenshots/i-5.png)
![3](Screenshots/i-4.png)
![4](Screenshots/i-3.png)
![5](Screenshots/i-2.png)

**5. Project Intelligence**

![1](Screenshots/pi-2.png)
![2](Screenshots/pi-1.png)
![3](Screenshots/pi-5.png)
![4](Screenshots/pi-4.png)
![5](Screenshots/pi-3.png)

**6. Price Benchmark**

![1](Screenshots/pb-2.png)
![2](Screenshots/pb-1.png)
![3](Screenshots/pb-5.png)
![4](Screenshots/pb-4.png)
![5](Screenshots/pb-3.png)

**7. Freshness**

![1](Screenshots/f-4.png)
![2](Screenshots/f-3.png)
![3](Screenshots/f-2.png)
![4](Screenshots/f-1.png)

**8. Data Quality**

![1](Screenshots/dq-4.png)
![2](Screenshots/dq-3.png)
![3](Screenshots/dq-2.png)
![4](Screenshots/dq-1.png)
![5](Screenshots/dq-5.png)

---

## ✅ Results

- Shipped an end-to-end Databricks Lakehouse project from scraping through governed Gold marts.

<img width="1017" height="379" alt="Pipeline Graph" src="https://github.com/user-attachments/assets/3367ee1e-5161-4637-9c63-9b09d038dac0" />


- Deployed the entire infrastructure — catalog setup, ingestion, and pipeline — using Databricks Asset Bundles, with zero manual UI configuration.

<img width="1108" height="362" alt="Deployment Status" src="https://github.com/user-attachments/assets/f088ef2a-6a66-4105-a3cd-4fc82031bf06" />


- Unified six independent ingestion channels (local upload, REST API, GitHub, Google Drive, Azure Blob, AWS S3) into a single Lakeflow streaming pipeline.

<img width="274" height="248" alt="volumes" src="https://github.com/user-attachments/assets/cf6f301f-0863-43a6-9a59-b8f9c769e0b9" />


- Processed roughly 18,000 residential property listings across six Indian cities.
- Built eight Gold materialized views, each mapped to a distinct analytical use case.
- Implemented automated data-quality validation with an explicit quarantine path for failing records.
- Produced a reusable, environment-parameterized deployment configuration ready for a production target.

---

## 💼 Skills Demonstrated

- Azure Databricks
- Databricks Asset Bundles (DAB)
- Lakeflow Declarative Pipelines
- Unity Catalog
- Databricks Volumes
- Auto Loader
- Delta Lake
- Streaming ETL
- PySpark
- SQL
- Data Quality Engineering
- Materialized Views
- Git & GitHub
- Cloud Data Engineering (Azure Blob Storage, AWS S3)
- Multi-source system integration (REST/Files APIs, Google Drive API, GitHub Contents API)

---

## Future Improvements

- CI/CD via GitHub Actions (`bundle validate` + `bundle deploy` on push)
- Scheduled runs instead of manual trigger (`schedule:` block in `mb_jobs.yml`)
- Incremental/CDC ingestion instead of one-time load
- Monitoring & alerting on pipeline and Job failures
- Unit tests for the Silver cleaning/validation logic
- A `prod` target with its own catalog and workspace

---

## Author

**Neel Kalyani**
Systems Engineer (Data Engineering), Infosys Ltd.
M.Sc. Data Science & Big Data Analytics

---

## 📄 License

This project is provided for educational and portfolio purposes.
