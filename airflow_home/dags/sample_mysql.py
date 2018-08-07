from datetime import datetime, timedelta
import json, logging
import csv
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from scipy.spatial.distance import cdist

from airflow.hooks import MySqlHook
from airflow.operators import PythonOperator, BranchPythonOperator
from airflow.models import DAG

log = logging.getLogger(__name__)

def simple_select_mysql(ds, **kwargs):
    mysql_hook = MySqlHook(mysql_conn_id="soletrade_localhost")
    users_trend = """SELECT 
                DISTINCT(email),
                sum(spent),
                count(transactions()
            FROM
            orders AS o
            GROUP BY 1
            ;"""
    with open('temp/output.csv', 'w') as f:
        wr = csv.writer(f, delimiter=',')
        csvRow = ["user", "spent", "transactions"]
        wr.writerow(csvRow)
        for user, spent, transactions in mysql_hook.get_records(users_trend):
            wr.writerow([user, spent, transactions])

def simple_elbow_data(ds, **kwargs):
    df = pd.read_csv('temp/output.csv')
    f1 = df['transactions'].values
    f2 = df['spent'].values
    X = np.array(list(zip(f1, f2)))
    distortions = []
    K = range(1,10)
    for k in K:
        kmeanModel = KMeans(n_clusters=k).fit(X)
        kmeanModel.fit(X)
        distortions.append(sum(np.min(cdist(X, kmeanModel.cluster_centers_, 'euclidean'), axis=1)) / X.shape[0])
    log.info(distortions)

def simple_kmeans_data(ds, **kwargs):
    df = pd.read_csv('temp/output.csv')
    f1 = df['transactions'].values
    f2 = df['spent'].values
    X = np.array(list(zip(f1, f2)))
    kmeans = KMeans(n_clusters=5).fit(X)
    log.info(kmeans.cluster_centers_)

dag = DAG('my_sql_test_dag', description='SQL tutorial DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2018, 3, 20), catchup=False)


simple_select_mysql_task = \
    PythonOperator(task_id='simple_select_mysql',
                   provide_context=True,
                   python_callable=simple_select_mysql,
                   dag=dag)

simple_elbow_data_task = PythonOperator(task_id='simple_elbow_data_task', provide_context=True, python_callable=simple_elbow_data, dag=dag)

simple_kmeans_data_task = PythonOperator(task_id='simple_kmeans_data_task', provide_context=True, python_callable=simple_kmeans_data, dag=dag)

simple_select_mysql_task >> simple_elbow_data_task
simple_select_mysql_task >> simple_kmeans_data_task