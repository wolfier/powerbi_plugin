from airflow.models import Variable
from powerbi_plugin.operators.dataset_in_group import PowerBIDatasetInGroupRefreshOperator
from datetime import datetime
from airflow.models import DAG

dag = DAG(
    dag_id='my_dag',
    schedule_interval='@once',
    start_date=datetime(2020, 1, 1)
)

dataset_info = [
    ("group_id_1", "dataset_id_1", "my_dataset"),
    ("group_id_2", "dataset_id_2", "dataset_name_2"),
]

with dag:
    for group_id, dataset_id, dataset_name in dataset_info:
        refresh_dataset = PowerBIDatasetInGroupRefreshOperator(
            task_id=f"refresh_{dataset_name}",
            group_id=group_id,
            dataset_id=dataset_id,
            # tenant_id=Variable.get('tenant_id'),
            # client_id=Variable.get('client_id'),
            # client_secret=Variable.get('client_secret'),
            powerbi_conn_id='powerbi_default',
            email=["name@domain.com"],
            email_on_failure=True,
        )

