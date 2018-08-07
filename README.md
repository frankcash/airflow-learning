# airflow-learning

### Getting dependencies installed

```
$ cd /path/to/my/airflow_learning/
$ virtualenv -p `which python3` venv
$ source venv/bin/activate
(venv) $ pip install -r requirements.txt
(venv) $ airflow initdb
```

### Running the web server

```
$ cd /path/to/my/airflow/workspace
$ source venv/bin/activate
(venv) $ export AIRFLOW_HOME=`pwd`/airflow_home
(venv) $ airflow webserver
```

### Running the scheduler

```
$ cd /path/to/my/airflow/workspace
$ export AIRFLOW_HOME=`pwd`/airflow_home
$ source venv/bin/activate
(venv) $ airflow scheduler
```

### Adding database
Go to the configuration tab underneath admin to add a database connection.