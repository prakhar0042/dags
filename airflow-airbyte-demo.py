import requests
import requests.auth

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.python import get_current_context
from airflow.models.param import Param

url = "http://localhost:8000/api/v1/connections/list"
post_data = {"workspaceId": "769fd779-88dd-40e2-8cc0-e0268dad9117"}
username = "prakhar.srivastav@finarkein.com"
password = "8DzIx1LqVNtWg5ekktIH9Ys985LfwPVL"

response = requests.post(
    url, json = post_data, auth = requests.auth.HTTPBasicAuth(username, password)
).json()

connections = {"d42ebce0-46ee-447b-bdd9-e3b30688cf0b":"Demo 6",
               "d7f499f1-315d-4f6b-8137-7be948c6ab3f":"Demo 1",
               "6f32576f-4806-4a87-8518-75d7957dff5b":"Demo 3",
               "f3a692a0-a989-4f3c-b143-2257f46782cb":"Demo 5",
               "1e2f32aa-1214-4dd1-9646-f8c5e58b905d":"Demo 2",
               "63a0f92d-6623-40cf-901f-e0160cedad0c":"Demo 7",
               "bb746c31-1b2d-4541-a4e6-6f413d3a87f9":"Demo 4"}

# for connection in response["connections"]:
#     connections[connection['connectionId']] = connection['name']

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

    # airbyte_call = AirbyteTriggerSyncOperator.partial(
    #     task_id = "airbyte_call",
    #     airbyte_conn_id = "airflow-call-to-airbyte-demo",
    #     asynchronous = False,
    #     max_active_tis_per_dag = "{{params.workers}}",
    #     map_index_template = "{{params.connections[task.connection_id]}}",
    # ).expand(connection_id = get_connection_ids())

    completion_output = BashOperator(
        task_id = "airbyte_completed_sync", bash_command="echo SYNC COMPLETED"
    )

    # airbyte_call >> completion_output
