from datetime import datetime
import time
import requests
import requests.auth

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.models.param import Param


url='http://localhost:8000/api/v1/connections/list'

post_data={"workspaceId":"f97ed1d1-3d06-4612-92c2-df50b0386e0c"}

username="airbyte"
password="password"

# response=requests.post(url,json=post_data,auth=requests.auth.HTTPBasicAuth(username,password)).json()

commands=[]

# for connection in response['connections']:
#     commands.append("echo hello {}".format(connection['name']))


workers=3

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="demo_next_xiii", start_date=datetime(2024, 7, 26,14,33), schedule="*/1 * * * *",
                 params={"commands":Param(commands,type='array',title="Bash commands")},render_template_as_native_obj=True
 ) as dag:
    # Tasks are represented as operators

    # tasks=BashOperator.partial(task_id='test',max_active_tis_per_dag=workers,map_index_template='{{task.bash_command}}').expand(bash_command=commands)

    hello = BashOperator(task_id="hello", bash_command="echo hello world")

    @task()
    def airflow1():
        time.sleep(5)
        print("airflow1 is working")

    @task()
    def airflow2():
        time.sleep(5)
        print("airflow2 is working")
    
    @task()
    def airflow3():
        time.sleep(5)
        print("airflow3 is working")
    
    @task()
    def airflow4():
        time.sleep(12)
        print("airflow4 is working")
    
    @task()
    def airflow5():
        time.sleep(15)
        print("airflow5 is working")
    
    @task()
    def airflow6():
        time.sleep(3)
        print("airflow6 is working")
    
    @task()
    def airflow7():
        time.sleep(10)
        print("airflow7 is working")

    @task()
    def airflow8():
        time.sleep(5)
        print("airflow8 is working")

    @task()
    def airflow9():
        time.sleep(8)
        print("airflow9 is working")

    bye = BashOperator(task_id="bye", bash_command="echo bye world")

    # Set dependencies between tasks
    hello >> airflow1() >> airflow2() >> [airflow3(), airflow4(), airflow5(), airflow6(), airflow7(), airflow8(), airflow9()] >> bye
    # tasks>>bye