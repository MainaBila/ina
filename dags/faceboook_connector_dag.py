from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta

default_args = {
    'owner': 'mibrahimbila',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 23),
    'email': ['mibrahimbila@klox.fr'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5)
}
dag = DAG(
    'faceboook_connector',
    default_args=default_args,
    description='An Airflow DAG to invoke simple dbt commands',
    schedule_interval='@once',
)

VIRTUAL_ENV_ACTIVATION = "/home/klox-dev/.venv/bin/activate"



CMD_FACEBOOK_CONNECTOR="python main.py "

FACEBOOK_CONNECTOR_DIR="/home/klox-dev/facebook-api-python-test"


facebook_connector_get_insights = BashOperator(
    task_id='facebook_connector_get_insights',
    bash_command=""" 
    source {VIRTUAL_ENV_ACTIVATION};
    cd {DIR} ;
    {CMD};
    """.format(VIRTUAL_ENV_ACTIVATION=VIRTUAL_ENV_ACTIVATION, DIR=FACEBOOK_CONNECTOR_DIR,CMD=CMD_FACEBOOK_CONNECTOR),
    dag=dag
)



facebook_connector_get_insights