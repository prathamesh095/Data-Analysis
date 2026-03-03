from pathlib import Path

PROJECT_NAME = "churn-retention-analytics"

FOLDERS = [
    "data/raw",
    "data/processed",
    "notebooks",
    "sql",
    "pipeline",
    "dashboard",
    "docs",
]

FILES = [
    "notebooks/00_data_profiling.ipynb",
    "notebooks/01_data_normalization.ipynb",
    "notebooks/02_data_quality_framework.ipynb",
    "notebooks/03_customer_features.ipynb",
    "notebooks/04_cohort_analysis.ipynb",
    "notebooks/05_statistical_validation.ipynb",
    "sql/00_config.sql",
    "sql/01_rfm_segmentation.sql",
    "sql/02_risk_engine.sql",
    "sql/03_revenue_exposure.sql",
    "sql/04_early_warning_alerts.sql",
    "pipeline/run_pipeline.py",
    "docs/business_problem.md",
    "docs/data_dictionary.md",
    "docs/statistical_findings.md",
    "docs/assumptions.md",
    "docs/database_ERD.png",
    "requirements.txt",
    ".env.example",
    "README.md",
]


def create_structure():
    root = Path(PROJECT_NAME)

    # Create root
    root.mkdir(exist_ok=True)

    # Create folders
    for folder in FOLDERS:
        (root / folder).mkdir(parents=True, exist_ok=True)

    # Create empty files
    for file in FILES:
        file_path = root / file
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.touch(exist_ok=True)

    print(f"✅ Project structure created at: {root.resolve()}")


if __name__ == "__main__":
    create_structure()