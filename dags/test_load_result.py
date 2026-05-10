import pandas as pd

from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param
from sqlalchemy import text
from dag import log_etl_event


default_args = {
  'owner': 'shurka',
  'depends_on_past': False,
  'start_date': None,
  'email': ['trambukin.a@mail.ru'],
  'schedule_interval': None,
  'tags': ['study']
}

dag = DAG(
    dag_id='test_load',
    default_args=default_args,
    description='Load data from csv to Postgres',
    params={"report_date": Param("2018-02-01", type="string", description="Дата формирования (1-е число месяца)")}
)


def test_import_f101_v2(**kwargs):
    start_time = datetime.now()
    task_id = kwargs['ti'].task_id
    report_date = kwargs['params']['report_date']
    target_table = 'dm_f101_round_f_v2'
    file_name = f'f101_report_{report_date}.csv'
    file_path = f'/opt/airflow/project_files/{file_name}'

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    engine = pg_hook.get_sqlalchemy_engine()

    try:
        import os
        if not os.path.exists(file_path):
            log_etl_event(target_table, start_time, task_id, status='FAILED', error=f"Файл не найден: {file_path}")
            return

        df = pd.read_csv(file_path, sep=';', encoding='utf-8-sig', dtype=str)
        rows_found = len(df)

        if rows_found > 0:
            with engine.begin() as conn:
                conn.execute(text(f'TRUNCATE TABLE dm."{target_table}" CASCADE'))
                df.to_sql(target_table, conn, schema='dm', if_exists='append', index=False)

            log_etl_event(target_table, start_time, task_id, rows=rows_found, status='SUCCESS')
        else:
            log_etl_event(target_table, start_time, task_id, rows=0, status='EMPTY_FILE')

    except Exception as e:
        log_etl_event(target_table, start_time, task_id, status='FAILED', error=str(e))
        raise e


task_import_check = PythonOperator(
        task_id='import_f101_test',
        python_callable=test_import_f101_v2,
        op_kwargs={
            'table': 'f101_report_{{ params.report_date }}',
            'mode': 'overwrite'
        },
        dag=dag
    )