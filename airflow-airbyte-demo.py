import requests
import requests.auth

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.python import get_current_context
from airflow.models.param import Param

url = "http://host.docker.internal:8000/api/v1/connections/list"
post_data = {"workspaceId": "769fd779-88dd-40e2-8cc0-e0268dad9117"}
username = "prakhar.srivastav@finarkein.com"
password = "8DzIx1LqVNtWg5ekktIH9Ys985LfwPVL"

response = requests.post(
    url, json = post_data, auth = requests.auth.HTTPBasicAuth(username, password)
).json()

connections = {}

for connection in response["connections"]:
    connections[connection['connectionId']] = connection['name']

workers = 2

with DAG(
    dag_id="airflow_airbyte",
    default_args={"owner": "airflow"},
    params={"connections": Param(connections, type = "object", title = "Airbyte connections"), "workers":Param(workers, type = "integer", title = "Workers")},
    render_template_as_native_obj=True
) as dag:
    
    @task
    def get_connection_ids():
        context = get_current_context()
        return list(context["params"]["connections"].keys())

    airbyte_call = AirbyteTriggerSyncOperator.partial(
        task_id = "airbyte_call",
        airbyte_conn_id = "airflow-call-to-airbyte-demo",
        asynchronous = False,
        max_active_tis_per_dag = "{{params.workers}}",
        map_index_template = "{{params.connections[task.connection_id]}}",
    ).expand(connection_id = get_connection_ids())

    completion_output = BashOperator(
        task_id = "airbyte_completed_sync", bash_command="echo SYNC COMPLETED"
    )

    airbyte_call >> completion_output
