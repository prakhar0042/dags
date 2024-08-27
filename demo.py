from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from airflow.models.param import Param

commands=["echo airflow", "echo finarkein", "echo test", "echo development", "echo DAG", "echo kubernetes", "echo minikube"]

workers=3

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="demo_DAG", params={"commands":Param(commands,type='array',title="Bash commands")},render_template_as_native_obj=True
 ) as dag:
    # Tasks are represented as operators

    @task
    def get_commands():
        context = get_current_context()
        return context["params"]["commands"]

    hello = BashOperator(task_id="hello", bash_command="echo hello world")

    tasks=BashOperator.partial(task_id='test',max_active_tis_per_dag=workers,map_index_template='{{task.bash_command}}').expand(bash_command=get_commands())

    bye = BashOperator(task_id="bye", bash_command="echo bye world")

    # Set dependencies between tasks
    hello>>tasks>>bye