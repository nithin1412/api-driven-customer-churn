# API-Driven Cloud Native Data Science Application
## Prefect Cloud + FastAPI — Customer Churn Analytics

This project is designed directly around the AIMLCZG549 Assignment-I requirements.

### Business problem
A telecom company wants to identify customers who are likely to churn so that retention teams can prioritize outreach. The pipeline downloads a public Telco Customer Churn dataset, cleans and normalizes it, performs EDA, computes correlations and feature importance, and publishes machine-readable results through a FastAPI service.

### Assignment coverage

| Requirement | Implementation |
|---|---|
| Business understanding | Customer churn prediction |
| Public dataset | IBM Telco Customer Churn CSV |
| Data ingestion | `ingest_data` Prefect task |
| Summary statistics | `preprocess_data` task + JSON artifact |
| Missing values | Missing-value report + median/mode imputation |
| Data types | Data-type report |
| Normalization | StandardScaler |
| Correlation | Correlation matrix |
| Numeric/categorical relationships | Grouped churn rates + encoded correlations |
| Binning | Tenure and MonthlyCharges bins |
| Encoding | One-hot encoding |
| Feature importance | Random Forest |
| Univariate/bivariate charts | Histogram, count plot, box plot, heatmap |
| DataOps | Prefect flow with tasks |
| Schedule | Every 2 minutes |
| Cloud dashboard | Prefect Cloud |
| API access | FastAPI |
| Built-in Prefect APIs | Prefect Python client in `/api/prefect-details` |
| Four+ application details | Flow, deployment, latest run, schedule/work pool |
| API testing | `curl` / Postman examples below |

### Architecture

Public CSV
   |
   v
Prefect Cloud deployment
   |
   +--> Ingest
   +--> Validate / clean
   +--> EDA
   +--> Feature engineering
   +--> Feature importance
   +--> Save artifacts
   |
   v
outputs/
   |
   v
FastAPI
   |
   +--> /api/summary
   +--> /api/missing-values
   +--> /api/correlation
   +--> /api/feature-importance
   +--> /api/prefect-details

Prefect Cloud provides orchestration, scheduling, run history, logs, and monitoring. A Prefect worker/work pool performs the actual Python execution; Cloud itself is the orchestration/control plane.

---

# 1. Setup

## Windows PowerShell

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Linux/macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create `.env` from `.env.example`.

---

# 2. Connect to Prefect Cloud

Create/login to your Prefect Cloud account at:

https://app.prefect.cloud/

Then authenticate the local Prefect CLI using the login command offered by your installed Prefect version, or set the Prefect Cloud API URL and API key in your environment.

Recommended check:

```bash
prefect version
prefect profile inspect
```

Do not commit API keys to GitHub.

---

# 3. Create a Cloud execution work pool

Create a Prefect Cloud-compatible work pool from the Prefect CLI/UI and name it:

```text
assignment-cloud-pool
```

For a fully cloud-hosted execution setup, choose a managed/push-style pool if that option is available in your Prefect Cloud workspace.

If your workspace requires a worker, start the worker from a cloud VM/container rather than your laptop:

```bash
prefect worker start --pool assignment-cloud-pool
```

The important distinction for the report is:

- Prefect Cloud = orchestration/control plane, dashboard, schedules, logs and run states.
- Work pool/worker = execution environment where pandas/sklearn actually process the dataset.

---

# 4. Deploy the pipeline

Edit `prefect.yaml` if your work pool has a different name.

```bash
prefect deploy
```

Or, if your Prefect version supports the deployment command generated from `prefect.yaml`, use the generated deployment command shown by the CLI.

After deployment, open Prefect Cloud and verify:

1. Flow exists.
2. Deployment exists.
3. Schedule is enabled.
4. A run starts every 2 minutes.
5. Logs are visible.
6. Task states can be inspected.

---

# 5. Run once manually

Use the Prefect Cloud deployment UI or CLI to trigger a run.

For local development:

```bash
python flows/pipeline.py
```

---

# 6. Start the API

```bash
uvicorn app.main:app --reload --port 8000
```

Open:

```text
http://127.0.0.1:8000/docs
```

FastAPI automatically provides Swagger documentation.

---

# 7. API test examples

### Summary

```bash
curl http://127.0.0.1:8000/api/summary
```

Expected HTTP status: `200`

### Missing values

```bash
curl http://127.0.0.1:8000/api/missing-values
```

### Correlation

```bash
curl http://127.0.0.1:8000/api/correlation
```

### Feature importance

```bash
curl http://127.0.0.1:8000/api/feature-importance
```

### Prefect application details

```bash
curl http://127.0.0.1:8000/api/prefect-details
```

The last endpoint uses the Prefect Python client to query the Prefect API for application/deployment/run information.

---

# 8. Screenshots required for submission

Capture these for the assignment report:

1. Prefect Cloud workspace overview.
2. Flow page.
3. Deployment page showing 2-minute schedule.
4. Successful flow run.
5. Flow/task logs.
6. Task graph/run details.
7. API Swagger page at `/docs`.
8. Successful `/api/summary` response.
9. Successful `/api/prefect-details` response.
10. Postman or Swagger response showing HTTP 200.

---

# 9. Suggested demonstration sequence

1. Explain the churn business problem.
2. Show the public dataset URL in `flows/pipeline.py`.
3. Open Prefect Cloud.
4. Show the deployed flow and 2-minute schedule.
5. Trigger a run.
6. Show task execution and logs.
7. Open the generated output files.
8. Start FastAPI.
9. Open Swagger.
10. Call `/api/summary`.
11. Call `/api/prefect-details`.
12. Explain that the Prefect API provides flow/deployment/run metadata.

---

# 10. Group contribution template

Replace the placeholders before submission.

- Member 1 — Business understanding, dataset selection, ingestion.
- Member 2 — Preprocessing, missing-value handling, normalization.
- Member 3 — EDA, feature engineering, feature importance.
- Member 4 — Prefect Cloud orchestration, API development, testing/documentation.

If there are fewer than four members, merge responsibilities accordingly.

---

# Dataset source

The pipeline uses the public IBM Telco Customer Churn CSV:

https://raw.githubusercontent.com/IBM/employee-attrition-aif360/master/data/WA_Fn-UseC_-Telco-Customer-Churn.csv

If that mirror becomes unavailable, replace `DATA_URL` in `flows/pipeline.py` with another public mirror of the same Telco Customer Churn dataset.

---

# Outputs

Every successful pipeline run creates:

- `outputs/cleaned_data.csv`
- `outputs/summary_statistics.json`
- `outputs/missing_values.json`
- `outputs/data_types.json`
- `outputs/correlation_matrix.json`
- `outputs/churn_rates_by_category.json`
- `outputs/feature_importance.json`
- `outputs/eda/tenure_distribution.png`
- `outputs/eda/churn_distribution.png`
- `outputs/eda/monthly_charges_by_churn.png`
- `outputs/eda/correlation_heatmap.png`
- `outputs/run_metadata.json`

These outputs make the project easy to demonstrate and document.

## Important

The assignment says the workflow should run every 2 minutes and log activity on a cloud dashboard. The `prefect.yaml` contains the 120-second schedule. You still need to connect the deployment to your own Prefect Cloud workspace and execution pool because account credentials and workspace identifiers are private.
