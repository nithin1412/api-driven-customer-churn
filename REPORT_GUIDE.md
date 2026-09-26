# Assignment Report Guide

Use this file to prepare the required Word/PDF report.

## 1. Title
API-Driven Cloud Native Data Science Application using Prefect Cloud

## 2. Business Understanding
Problem: Predict/understand telecom customer churn so retention teams can identify customer segments with higher churn rates.

Business objective:
- Clean customer data automatically.
- Understand factors associated with churn.
- Produce repeatable analytics.
- Orchestrate the pipeline every 2 minutes.
- Expose application and pipeline information through APIs.

## 3. Dataset
Dataset: IBM Telco Customer Churn.
Public CSV source is configured in `flows/pipeline.py`.

Include a screenshot of the dataset/source.

## 4. Data Pipeline
Explain:
1. Ingestion
2. Missing-value detection
3. Numeric imputation
4. Data-type inspection
5. Normalization
6. Binning
7. Encoding
8. Correlation analysis
9. Feature importance
10. Visualization
11. Output generation

## 5. DataOps / Prefect Cloud
Explain that Prefect Cloud is used for orchestration and monitoring.

Show screenshots:
- Flow
- Deployment
- 2-minute schedule
- Run history
- Successful run
- Logs
- Task states

## 6. API
The FastAPI application exposes:
- `/api/summary`
- `/api/missing-values`
- `/api/data-types`
- `/api/correlation`
- `/api/churn-rates`
- `/api/feature-importance`
- `/api/run-metadata`
- `/api/prefect-details`

For the assignment, emphasize `/api/prefect-details` because it uses the Prefect API/client to retrieve application details.

## 7. API testing table

| Endpoint | Method | Expected Status | Purpose |
|---|---|---:|---|
| `/api/summary` | GET | 200 | Summary statistics |
| `/api/missing-values` | GET | 200 | Missing-value report |
| `/api/correlation` | GET | 200 | Correlation matrix |
| `/api/feature-importance` | GET | 200 | Feature importance |
| `/api/prefect-details` | GET | 200 | Flow/deployment/run details |

Attach screenshots of requests and responses.

## 8. Group contribution
Replace the names and responsibilities with the actual group members.

## 9. Conclusion
State that the project demonstrates a repeatable cloud-orchestrated DataOps pipeline with API access and monitoring.
