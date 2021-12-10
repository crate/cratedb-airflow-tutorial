import sys
from airflow.models import DagBag

# ensure dags/config gets loaded
sys.path.insert(0, 'dags')

# Verifies that all DAGs can be loaded successfully
def test_no_import_errors():
    dag_bag = DagBag(dag_folder='dags', include_examples=False)
    assert len(dag_bag.import_errors) == 0, "No Import Failures"
