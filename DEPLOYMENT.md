# Deploying this project with Databricks Asset Bundles

## What's in the bundle

```
databricks.yml                                   # bundle root config
resources/
  mb_pipeline.yml                                 # Lakeflow Declarative Pipeline (Bronze -> Silver -> Gold)
  mb_jobs.yml                                      # orchestration job: setup -> ingest -> pipeline refresh
setup/
  00_Setup_Catalog.py                              # replaces Base_Creation.py; idempotent, parameterized
01_Data_Ingestion.py                                # GitHub / Google Drive / Azure Blob ingestion (now parameterized)
transformations/
  02_Bronze_Ingestion.py .. 06_Gold_Layer_*.py      # unchanged logic, catalog name is now a variable
```

**Not in the bundle:** `scripts/Main.py` and `scripts/rest_api.py`. These run on your
laptop against Magicbricks directly (not inside Databricks), so there's nothing for
a Databricks bundle to deploy — they stay exactly as they are, run manually or via
your own scheduler (e.g. Task Scheduler / cron) before the ingestion job runs.

## What changed in the existing files, and why

- Every `transformations/*.py` file previously hardcoded
  `CATALOG = "adb_real_estate_mb"` (or, in the Gold file, hardcoded the full
  three-part table name nine separate times with no variable at all). A bundle
  that can't change which catalog it points to isn't really doing its job, so
  these now read `spark.conf.get("catalog", "adb_real_estate_mb")` — the pipeline
  resource passes this in via `configuration:`, with the same default so the
  notebooks still run standalone in the UI if you ever need to.
- `01_Data_Ingestion.py` now reads the catalog from a notebook widget the same way,
  and the Google Drive `API_KEY` — which was a hardcoded placeholder string
  (`"Google_Console_API_KEY"`) even though your README says it comes from
  Databricks Secrets — now actually calls `dbutils.secrets.get()`. You'll need to
  create that secret scope/key before this task runs (Step 3 below).
- `Base_Creation.py` (raw SQL, no notebook header, so it couldn't be run as a job
  task anyway) is replaced by `setup/00_Setup_Catalog.py`. It also fixes a real
  mismatch: your old setup script created volumes named `raw.github`,
  `raw.google_drive`, `raw.rest_api`, `raw.local_upload`, but the Bronze
  ingestion notebook and the README both reference `github_chennai`,
  `google_drive_mumbai`, `rest_api_delhi_ncr`, `local_upload_amd`. The new
  script creates the names your pipeline actually reads from.

## Steps to implement

### 1. Install the CLI and authenticate
```bash
databricks -v   # need 0.283.0+; install/update from docs.databricks.com if older
databricks auth login --host https://<your-workspace-url>.cloud.databricks.com
```
Accept the default profile name (or name it e.g. `mb-dev`) when prompted.

### 2. Drop these files into your repo
Copy `databricks.yml`, `resources/`, `setup/`, and the updated `01_Data_Ingestion.py`
and `transformations/*.py` into the root of your existing repo, replacing the old
versions of those five transformation files and `01_Data_Ingestion.py`.
You can delete `Base_Creation.py` — it's superseded by `setup/00_Setup_Catalog.py`.

### 3. Create the one thing that has to stay manual
Unity Catalog catalog creation is account/metastore-level, so it's outside the
bundle on purpose:
```sql
CREATE CATALOG IF NOT EXISTS adb_real_estate_mb;
```
Also create the Google Drive API key secret once, since `01_Data_Ingestion.py`
now pulls it from Secrets instead of a hardcoded string:
```bash
databricks secrets create-scope mb_real_estate
databricks secrets put-secret mb_real_estate gdrive_api_key
```

### 4. Edit `databricks.yml`
Replace `<your-workspace-url>` under `targets.dev.workspace.host` with your real
workspace URL.

### 5. Validate
```bash
databricks bundle validate
```
Fix anything it flags before moving on.

### 6. Deploy
```bash
databricks bundle deploy --target dev
```
This uploads the notebooks to your workspace and creates the pipeline + job
(both in "paused/dev mode" — dev-target deploys don't auto-schedule anything).

### 7. Run it
```bash
databricks bundle run mb_end_to_end --target dev
```
This runs `setup_catalog` -> `ingest_external_sources` -> `refresh_medallion_pipeline`
in order. First run will take longer since Bronze streaming tables and Gold
materialized views are being created from scratch.

Before this, make sure the raw JSON files this pipeline expects are actually in
the volumes: run `scripts/Main.py` / `scripts/rest_api.py` locally for the cities
that use local upload, or make sure your GitHub/Drive/ADLS sources have data,
since `ingest_external_sources` only pulls from those three; the Bronze pipeline
picks up whatever lands in the volumes via Auto Loader.

### 8. Iterate
Any time you change a notebook or a resource YAML:
```bash
databricks bundle validate
databricks bundle deploy --target dev
```

### 9. Tear down (optional, for cost control)
```bash
databricks bundle destroy --target dev
```
This deletes the pipeline, job, and any tables/views it manages — type `y` to confirm.

## What this buys you for the CV line

"Deployed with Databricks Asset Bundles" now means something concrete you can
walk an interviewer through: one YAML-defined pipeline resource, one orchestration
job with an explicit task DAG (setup -> ingest -> transform), a single
`databricks bundle deploy` command instead of manual UI clicks, and an
environment-parameterized catalog instead of a string copy-pasted nine times.
If they ask "how would you add a prod target," the honest answer is: add a
`prod:` block under `targets` in `databricks.yml` with its own `workspace.host`
and `variables.catalog`, which is a legitimate answer to give live in an interview.
