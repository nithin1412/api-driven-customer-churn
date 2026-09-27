from pathlib import Path
import json
import os
from typing import Any

from fastapi import FastAPI, HTTPException
from prefect import get_client
from prefect.client.schemas.filters import FlowRunFilter, DeploymentFilter, FlowFilter
ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "outputs"

app = FastAPI(
    title="Customer Churn Cloud DataOps API",
    version="1.0.0",
    description="API access to pipeline analytics and Prefect Cloud application details.",
)


def load_json(name: str) -> Any:
    path = OUTPUT_DIR / name
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"{name} not available. Run the Prefect pipeline first.",
        )
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


@app.get("/")
def root():
    return {
        "application": "Customer Churn Cloud DataOps API",
        "status": "running",
        "docs": "/docs",
    }


@app.get("/api/summary")
def summary():
    return load_json("summary_statistics.json")


@app.get("/api/missing-values")
def missing_values():
    return load_json("missing_values.json")


@app.get("/api/data-types")
def data_types():
    return load_json("data_types.json")


@app.get("/api/correlation")
def correlation():
    return load_json("correlation_matrix.json")


@app.get("/api/churn-rates")
def churn_rates():
    return load_json("churn_rates_by_category.json")


@app.get("/api/feature-importance")
def feature_importance():
    return load_json("feature_importance.json")


@app.get("/api/run-metadata")
def run_metadata():
    return load_json("run_metadata.json")


@app.get("/api/prefect-details")
async def prefect_details():
    """
    Uses Prefect's built-in Python client, which communicates with the
    configured Prefect API/Prefect Cloud workspace.
    Returns 4+ application details required by the assignment.
    """
    flow_name = os.getenv("PREFECT_FLOW_NAME", "customer-churn-pipeline")
    deployment_name = os.getenv("PREFECT_DEPLOYMENT_NAME", "customer-churn-cloud")

    async with get_client() as client:
        flows = await client.read_flows(
            flow_filter=FlowFilter(name={"any_": [flow_name]})
        )

        deployments = await client.read_deployments(
            deployment_filter=DeploymentFilter(name={"any_": [deployment_name]})
        )

        recent_runs = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(),
            limit=5,
            sort="START_TIME_DESC",
        )

        return {
            "flow_name": flows[0].name if flows else flow_name,
            "flow_id": str(flows[0].id) if flows else None,
            "deployment_name": deployments[0].name if deployments else deployment_name,
            "deployment_id": str(deployments[0].id) if deployments else None,
            "work_pool": (
                deployments[0].work_pool_name
                if deployments else "assignment-cloud-pool"
            ),
            "schedule_interval_seconds": 120,
            "recent_runs": [
                {
                    "id": str(run.id),
                    "name": run.name,
                    "state": run.state.name if run.state else None,
                    "start_time": str(run.start_time),
                    "end_time": str(run.end_time),
                }
                for run in recent_runs
            ],
        }
