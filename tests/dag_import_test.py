"Global DAG import test to identify syntax or dependency issues"

from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = PROJECT_ROOT / "dags"


def test_all_dags_load(monkeypatch):
    monkeypatch.syspath_prepend(str(PROJECT_ROOT))

    dag_bag = DagBag(
        dag_folder=DAGS_DIR,
        safe_mode=False,
    )

    if dag_bag.import_errors:
        errors = "\n\n".join(
            f"{filename}:\n{traceback}"
            for filename, traceback in dag_bag.import_errors.items()
        )
        pytest.fail(f"DAG import errors:\n\n{errors}")

    assert dag_bag.dags, f"No DAGs discovered under {DAGS_DIR}"
