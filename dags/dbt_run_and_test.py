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
    'dbt_run_and_test_models',
    default_args=default_args,
    description='An Airflow DAG to invoke simple dbt commands',
    schedule_interval='@once',
)

VIRTUAL_ENV_ACTIVATION = "/home/klox-dev/.venv/bin/activate"
DBT_DIR = "/home/klox-dev/dbt/klox-dev"
CMD_RUN ="dbt run"
CMD_TEST ="dbt test"







dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=""" 
    source {VIRTUAL_ENV_ACTIVATION};
    cd {DBT_DIR} ;
    {CMD};
    """.format(VIRTUAL_ENV_ACTIVATION=VIRTUAL_ENV_ACTIVATION, DBT_DIR=DBT_DIR,CMD=CMD_RUN),
    dag=dag
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=""" 
    source {VIRTUAL_ENV_ACTIVATION};
    cd {DBT_DIR} ;
    {CMD};
    """.format(VIRTUAL_ENV_ACTIVATION=VIRTUAL_ENV_ACTIVATION, DBT_DIR=DBT_DIR,CMD=CMD_TEST),
    dag=dag
)

dbt_run >> dbt_test