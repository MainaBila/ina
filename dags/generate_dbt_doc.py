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
    'dbt_docs_generate',
    default_args=default_args,
    description='This DAG generate DBT documentation based on models and schema defined in dbt',
    schedule_interval='@once',
)

VIRTUAL_ENV_ACTIVATION = "/home/klox-dev/.venv/bin/activate"
DBT_DIR = "/home/klox-dev/dbt/klox-dev"
CMD_DBT_DOCS_GENERATE ="dbt docs generate "


dbt_docs_generate = BashOperator(
    task_id='dbt_docs_generate',
    bash_command=""" 
    source {VIRTUAL_ENV_ACTIVATION};
    cd {DBT_DIR} ;
    {CMD};
    """.format(VIRTUAL_ENV_ACTIVATION=VIRTUAL_ENV_ACTIVATION, DBT_DIR=DBT_DIR,CMD=CMD_DBT_DOCS_GENERATE),
    dag=dag
)
