from pathlib import Path
from datetime import datetime, timezone
import json
import os
import warnings

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from prefect import flow, task, get_run_logger
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "outputs"
EDA_DIR = OUTPUT_DIR / "eda"

DATA_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)
EDA_DIR.mkdir(exist_ok=True)

DATA_URL = os.getenv(
    "DATA_URL",
    "https://raw.githubusercontent.com/IBM/telco-customer-churn-on-icp4d/master/data/Telco-Customer-Churn.csv",
)


def write_json(filename: str, value):
    path = OUTPUT_DIR / filename
    with path.open("w", encoding="utf-8") as f:
        json.dump(value, f, indent=2, default=str)
    return str(path)


@task(retries=2, retry_delay_seconds=10, log_prints=True)
def ingest_data() -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Starting data ingestion from public repository")
    df = pd.read_csv(DATA_URL)
    logger.info("Downloaded %s rows and %s columns", len(df), len(df.columns))
    df.to_csv(DATA_DIR / "raw_telco_churn.csv", index=False)
    return df


@task(log_prints=True)
def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Starting preprocessing")

    # Convert TotalCharges to numeric because the source contains whitespace values.
    if "TotalCharges" in df.columns:
        df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce")

    missing_before = df.isna().sum().to_dict()
    write_json("missing_values_before.json", missing_before)

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    categorical_cols = df.select_dtypes(exclude="number").columns.tolist()

    for col in numeric_cols:
        if df[col].isna().any():
            df[col] = df[col].fillna(df[col].median())

    for col in categorical_cols:
        if df[col].isna().any():
            mode = df[col].mode()
            df[col] = df[col].fillna(mode.iloc[0] if not mode.empty else "Unknown")

    # Target encoding.
    if "Churn" in df.columns:
        df["ChurnFlag"] = df["Churn"].map({"Yes": 1, "No": 0}).astype(int)

    # Remove customer identifier from modeling.
    if "customerID" in df.columns:
        df = df.drop(columns=["customerID"])

    write_json(
        "summary_statistics.json",
        json.loads(df.describe(include="all").fillna("").to_json()),
    )
    write_json(
        "missing_values.json",
        df.isna().sum().to_dict(),
    )
    write_json(
        "data_types.json",
        {k: str(v) for k, v in df.dtypes.items()},
    )

    # Normalization of selected numeric features.
    scale_cols = [c for c in ["tenure", "MonthlyCharges", "TotalCharges"] if c in df.columns]
    if scale_cols:
        scaler = StandardScaler()
        df[[f"{c}_scaled" for c in scale_cols]] = scaler.fit_transform(df[scale_cols])

    df.to_csv(OUTPUT_DIR / "cleaned_data.csv", index=False)
    logger.info("Preprocessing completed")
    return df


@task(log_prints=True)
def exploratory_data_analysis(df: pd.DataFrame) -> dict:
    logger = get_run_logger()
    logger.info("Starting exploratory data analysis")

    # Binning
    if "tenure" in df.columns:
        df["TenureBand"] = pd.cut(
            df["tenure"],
            bins=[-1, 12, 24, 48, 72, float("inf")],
            labels=["0-12", "13-24", "25-48", "49-72", "73+"],
        )

    if "MonthlyCharges" in df.columns:
        df["MonthlyChargeBand"] = pd.cut(
            df["MonthlyCharges"],
            bins=[-float("inf"), 40, 70, 100, float("inf")],
            labels=["Low", "Medium", "High", "Very High"],
        )

    # Correlations for numeric variables.
    numeric_df = df.select_dtypes(include="number")
    corr = numeric_df.corr(numeric_only=True).round(4)
    write_json("correlation_matrix.json", json.loads(corr.to_json()))

    # Categorical churn rates.
    churn_rates = {}
    if "Churn" in df.columns:
        for col in ["Contract", "InternetService", "PaymentMethod", "TenureBand", "MonthlyChargeBand"]:
            if col in df.columns:
                grouped = (
                    df.groupby(col, observed=False)["ChurnFlag"]
                    .mean()
                    .mul(100)
                    .round(2)
                    .to_dict()
                )
                churn_rates[col] = grouped
    write_json("churn_rates_by_category.json", churn_rates)

    sns.set_theme()
    if "tenure" in df.columns:
        plt.figure(figsize=(8, 5))
        sns.histplot(df["tenure"], bins=20)
        plt.title("Customer Tenure Distribution")
        plt.xlabel("Tenure (months)")
        plt.tight_layout()
        plt.savefig(EDA_DIR / "tenure_distribution.png", dpi=140)
        plt.close()

    if "Churn" in df.columns:
        plt.figure(figsize=(6, 5))
        sns.countplot(data=df, x="Churn")
        plt.title("Churn Distribution")
        plt.tight_layout()
        plt.savefig(EDA_DIR / "churn_distribution.png", dpi=140)
        plt.close()

    if "MonthlyCharges" in df.columns and "Churn" in df.columns:
        plt.figure(figsize=(7, 5))
        sns.boxplot(data=df, x="Churn", y="MonthlyCharges")
        plt.title("Monthly Charges by Churn")
        plt.tight_layout()
        plt.savefig(EDA_DIR / "monthly_charges_by_churn.png", dpi=140)
        plt.close()

    if not corr.empty:
        plt.figure(figsize=(12, 8))
        sns.heatmap(corr, cmap="coolwarm", center=0)
        plt.title("Numeric Feature Correlation Heatmap")
        plt.tight_layout()
        plt.savefig(EDA_DIR / "correlation_heatmap.png", dpi=140)
        plt.close()

    logger.info("EDA completed")
    return {"numeric_features": list(numeric_df.columns), "churn_rate_groups": churn_rates}


@task(log_prints=True)
def feature_importance(df: pd.DataFrame) -> dict:
    logger = get_run_logger()
    logger.info("Calculating feature importance using Random Forest")

    if "ChurnFlag" not in df.columns:
        raise ValueError("ChurnFlag target is missing")

    y = df["ChurnFlag"]
    X = df.drop(columns=["Churn", "ChurnFlag"], errors="ignore")

    # One-hot encode categorical variables.
    X = pd.get_dummies(X, drop_first=True)
    X = X.replace([float("inf"), -float("inf")], pd.NA).fillna(0)

    model = RandomForestClassifier(
        n_estimators=200,
        random_state=42,
        n_jobs=-1,
        class_weight="balanced",
    )
    model.fit(X, y)

    importance = (
        pd.Series(model.feature_importances_, index=X.columns)
        .sort_values(ascending=False)
        .head(15)
        .round(6)
        .to_dict()
    )

    write_json("feature_importance.json", importance)
    logger.info("Top feature: %s", next(iter(importance)) if importance else "N/A")
    return importance


@task(log_prints=True)
def save_run_metadata(df: pd.DataFrame, importance: dict):
    logger = get_run_logger()
    metadata = {
        "pipeline": "customer-churn-pipeline",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "rows_processed": int(len(df)),
        "columns_processed": int(len(df.columns)),
        "top_feature": next(iter(importance)) if importance else None,
        "schedule_seconds": 120,
    }
    write_json("run_metadata.json", metadata)
    logger.info("Run metadata saved")
    return metadata


@flow(name="customer-churn-pipeline", log_prints=True)
def customer_churn_pipeline():
    logger = get_run_logger()
    logger.info("=== Customer Churn DataOps pipeline started ===")
    raw = ingest_data()
    cleaned = preprocess_data(raw)
    exploratory_data_analysis(cleaned)
    importance = feature_importance(cleaned)
    metadata = save_run_metadata(cleaned, importance)
    logger.info("=== Pipeline completed successfully ===")
    return metadata


if __name__ == "__main__":
    customer_churn_pipeline()
