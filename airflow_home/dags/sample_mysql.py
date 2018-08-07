from datetime import datetime, timedelta
import json, logging


from airflow.hooks import MySqlHook
from airflow.operators import PythonOperator
from airflow.models import DAG

log = logging.getLogger(__name__)

def simple_select_mysql(ds, **kwargs):
    mysql_hook = MySqlHook(mysql_conn_id="slocalhost")
    users_trend = """SELECT 
                DISTINCT(email),
                sum(spent),
                count(transactions()
            FROM
            orders AS o
            GROUP BY 1
            ;"""

    for user, spent, transactions in mysql_hook.get_records(users_trend):
        log.info(" {}: {} & {}".format(user, spent, transactions))


dag = DAG('my_sql_test_dag', description='SQL tutorial DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2018, 3, 20), catchup=False)


simple_select_mysql_task = \
    PythonOperator(task_id='simple_select_mysql',
                   provide_context=True,
                   python_callable=simple_select_mysql,
                   dag=dag)


simple_select_mysql_task