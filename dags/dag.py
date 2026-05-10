import pandas as pd
import numpy as np
import time

from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from sqlalchemy import text


config = {
    'ft_balance_f' : {
        'keys' : ['on_date', 'account_rk'],
        'mode' : 'upsert'
    },
    'md_account_d' : {
        'keys' : ['data_actual_date', 'account_rk'],
        'mode' : 'upsert'
    },
    'md_currency_d' : {
        'keys' : ['currency_rk', 'data_actual_date'],
        'mode' : 'upsert'
    },
    'md_exchange_rate_d' : {
        'keys' : ['data_actual_date', 'currency_rk'],
        'mode' : 'upsert'
    },
    'md_ledger_account_s': {
        'keys': ['ledger_account', 'start_date'],
        'mode': 'upsert'
    },
    'ft_posting_f': {
        'keys': None,
        'mode': 'overwrite'
    }
}

def log_etl_event(entity_name, start_time, task_id, rows=0, status='SUCCESS', error=None):
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')

    log_sql = """
        INSERT INTO logs.etl_log 
        (dag_id, task_id, entity_name, start_time, end_time, rows_affected, status, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    pg_hook.run(log_sql, parameters=(
        'data_load_csv',
        task_id,
        entity_name,
        start_time,
        datetime.now(),
        rows,
        status,
        str(error)[:500] if error else None
    ))

def load_data(table, pk_columns=None, mode='upsert', **kwargs):
    start_time = datetime.now()
    task_id = kwargs['ti'].task_id
    try:
        df = pd.read_csv(f'/opt/airflow/project_files/{table}.csv', dtype=str, encoding='utf-8-sig', encoding_errors='replace', sep=';')
        df.columns = [col.lower() for col in df.columns]
        df = df.replace(r'.*\ufffd.*', np.nan, regex=True)
        df = df.drop_duplicates()
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        engine = pg_hook.get_sqlalchemy_engine()
        for col in df.columns:
            df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
            df[col] = df[col].replace(['nan', 'None', ''], np.nan)
        for col in [c for c in df.columns if 'date' in c or 'dt' in c]:
            df[col] = pd.to_datetime(df[col], dayfirst=True, format='mixed', errors='coerce')
        rows_to_load = len(df)
        if mode == 'overwrite':
            with engine.begin() as conn:
                conn.execute(text(f'TRUNCATE TABLE ds."{table}" CASCADE'))
            df.to_sql(table, engine, schema='ds', if_exists='append', index=False)
        else:
            temp_table = f"temp_{table}"
            df.to_sql(temp_table, engine, schema='ds', if_exists='replace', index=False)
            type_query = f"""
                        SELECT attname, atttypid::regtype::text 
                        FROM pg_attribute 
                        WHERE attrelid = 'ds."{table}"'::regclass 
                        AND attname IN ({', '.join([f"'{c}'" for c in df.columns])})
                    """
            column_types = dict(pg_hook.get_records(type_query))
            select_parts = []
            for col in df.columns:
                target_type = column_types.get(col, 'text')
                select_parts.append(f'"{col}"::{target_type}')
            columns_list = [f'"{col}"' for col in df.columns]
            update_cols = [f'"{col}" = EXCLUDED."{col}"' for col in df.columns if col not in pk_columns]
            pk_str = ", ".join([f'"{col}"' for col in pk_columns])
            query = f"""
                    INSERT INTO ds."{table}" ({", ".join(columns_list)})
                    SELECT {", ".join(select_parts )} FROM ds."{temp_table}"
                    ON CONFLICT ({pk_str}) 
                    DO UPDATE SET {", ".join(update_cols)};
    
                    DROP TABLE ds."{temp_table}";
                    """
            pg_hook.run(query)
        time.sleep(5)
        log_etl_event(table, start_time, task_id, rows=rows_to_load, status='SUCCESS')
    except Exception as e:
        log_etl_event(table, start_time, task_id, status='FAILED', error=e)
        raise


def init_history_2017(**kwargs):
    start_time = datetime.now()
    task_id = kwargs['ti'].task_id
    entity_name = 'dm.dm_account_balance_f_history'
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')

    try:
        check_sql = "SELECT COUNT(*) FROM dm.dm_account_balance_f WHERE on_date = '2017-12-31'"
        count = pg_hook.get_first(check_sql)[0]

        if count == 0:
            insert_sql = """
                INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
                SELECT 
                    fbf.on_date, fbf.account_rk, fbf.balance_out,
                    fbf.balance_out * COALESCE(merd.reduced_cource, 1)
                FROM ds.ft_balance_f fbf 
                LEFT JOIN ds.md_account_d mad 
                    ON fbf.account_rk = mad.account_rk 
                    AND fbf.on_date BETWEEN mad.data_actual_date AND mad.data_actual_end_date
                LEFT JOIN ds.md_exchange_rate_d merd  
                    ON mad.currency_rk = merd.currency_rk 
                    AND fbf.on_date BETWEEN merd.data_actual_date AND merd.data_actual_end_date
                WHERE fbf.on_date = '2017-12-31';
            """
            pg_hook.run(insert_sql)
            new_count = pg_hook.get_first(check_sql)[0]
            log_etl_event(entity_name, start_time, task_id, rows=new_count, status='SUCCESS')
        else:
            log_etl_event(entity_name, start_time, task_id, rows=0, status='SUCCESS')

    except Exception as e:
        log_etl_event(entity_name, start_time, task_id, status='FAILED', error=e)
        raise e


def export_f101_to_csv(**kwargs):
    start_time = datetime.now()
    task_id = kwargs['ti'].task_id
    report_date = kwargs['params']['report_date']
    table_name = 'dm_f101_round_f'

    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        engine = pg_hook.get_sqlalchemy_engine()

        query = f"""
            SELECT * FROM dm.{table_name} 
            WHERE to_date = ('{report_date}'::DATE - INTERVAL '1 day')::DATE
        """
        df = pd.read_sql(query, engine)
        rows_exported = len(df)

        file_path = f'/opt/airflow/project_files/f101_report_{report_date}.csv'
        df.to_csv(file_path, index=False, sep=';', encoding='utf-8-sig')

        log_etl_event(table_name, start_time, task_id, rows=rows_exported, status='SUCCESS')
        print(f"Успешно выгружено {rows_exported} строк в {file_path}")

    except Exception as e:
        log_etl_event(table_name, start_time, task_id, status='FAILED', error=e)
        raise e


default_args = {
  'dag_id': 'Neo_homework',
  'owner': 'shurka',
  'depends_on_past': False,
  'start_date': None,
  'email': ['trambukin.a@mail.ru'],
  'schedule_interval': None,
  'tags': ['study']
}

dag = DAG(
    dag_id='data_load_csv',
    default_args=default_args,
    description='Load data from csv to Postgres',
    params={"report_date": Param("2018-02-01", type="string", description="Дата формирования (1-е число месяца)")}
)

insert_ft_balance_f = PythonOperator(
    task_id='insert_ft_balance_f',
    python_callable=load_data,
    op_kwargs={'table': 'ft_balance_f',
               'pk_columns': config['ft_balance_f']['keys'],
               'mode': config['ft_balance_f']['mode']},
    dag=dag
)

insert_ft_posting_f = PythonOperator(
    task_id='insert_ft_posting_f',
    python_callable=load_data,
    op_kwargs={'table': 'ft_posting_f',
               'pk_columns': config['ft_posting_f']['keys'],
               'mode': config['ft_posting_f']['mode']},
    dag=dag
)

insert_md_account_d = PythonOperator(
    task_id='insert_md_account_d',
    python_callable=load_data,
    op_kwargs={'table': 'md_account_d',
               'pk_columns': config['md_account_d']['keys'],
               'mode': config['md_account_d']['mode']},
    dag=dag
)

insert_md_currency_d = PythonOperator(
    task_id='insert_md_currency_d',
    python_callable=load_data,
    op_kwargs={'table': 'md_currency_d',
               'pk_columns': config['md_currency_d']['keys'],
               'mode': config['md_currency_d']['mode']},
    dag=dag
)

insert_md_exchange_rate_d = PythonOperator(
    task_id='insert_md_exchange_rate_d',
    python_callable=load_data,
    op_kwargs={'table': 'md_exchange_rate_d',
               'pk_columns': config['md_exchange_rate_d']['keys'],
               'mode': config['md_exchange_rate_d']['mode']},
    dag=dag
)

insert_md_ledger_account_s = PythonOperator(
    task_id='insert_md_ledger_account_s',
    python_callable=load_data,
    op_kwargs={'table': 'md_ledger_account_s',
               'pk_columns': config['md_ledger_account_s']['keys'],
               'mode': config['md_ledger_account_s']['mode']},
    dag=dag
)

task_init_history = PythonOperator(
    task_id='init_history_2017',
    python_callable=init_history_2017
)


task_turnover = SQLExecuteQueryOperator(
    task_id='fill_turnover_monthly',
    conn_id='postgres_conn',
    sql="""
    DO $$
    DECLARE

        v_date DATE;
        v_start_date DATE := date_trunc('month', '{{ params.report_date }}'::DATE - INTERVAL '1 day')::DATE;
        v_end_date   DATE := ('{{ params.report_date }}'::DATE - INTERVAL '1 day')::DATE;
        v_log_id INT;
    BEGIN
        v_date := v_start_date;

        WHILE v_date <= v_end_date LOOP
        
            INSERT INTO logs.etl_log (dag_id, task_id, entity_name, start_time, status)
            VALUES ('{{ dag.dag_id }}', '{{ task.task_id }}', 'turnover_' || v_date, NOW(), 'RUNNING')
            RETURNING id INTO v_log_id;

            CALL ds.fill_account_turnover_f(v_date);

            UPDATE logs.etl_log 
            SET end_time = NOW(), 
                status = 'SUCCESS' 
            WHERE id = v_log_id;

            v_date := v_date + INTERVAL '1 day';
        END LOOP;
    END $$;
    """
)

task_balance = SQLExecuteQueryOperator(
    task_id='fill_balance_monthly',
    conn_id='postgres_conn',
    sql="""
    DO $$
    DECLARE
        v_date DATE;
        v_start_date DATE := date_trunc('month', '{{ params.report_date }}'::DATE - INTERVAL '1 day')::DATE;
        v_end_date   DATE := ('{{ params.report_date }}'::DATE - INTERVAL '1 day')::DATE;
        v_log_id INT;
    BEGIN
        v_date := v_start_date;

        WHILE v_date <= v_end_date LOOP
            INSERT INTO logs.etl_log (dag_id, task_id, entity_name, start_time, status)
            VALUES ('{{ dag.dag_id }}', '{{ task.task_id }}', 'balance_day_' || v_date, NOW(), 'RUNNING')
            RETURNING id INTO v_log_id;

            CALL ds.fill_account_balance_f(v_date);

            UPDATE logs.etl_log 
            SET end_time = NOW(), 
                status = 'SUCCESS' 
            WHERE id = v_log_id;

            v_date := v_date + INTERVAL '1 day';
        END LOOP;
    EXCEPTION WHEN OTHERS THEN
        UPDATE logs.etl_log SET end_time = NOW(), status = 'ERROR', error_message = SQLERRM WHERE id = v_log_id;
        RAISE;
    END $$;
    """
)


task_f101 = SQLExecuteQueryOperator(
    task_id='fill_f101',
    conn_id='postgres_conn',
    sql="""
    DO $$
    DECLARE
        v_log_id INT;
    BEGIN

        INSERT INTO logs.etl_log (dag_id, task_id, entity_name, start_time, status)
        VALUES ('{{ dag.dag_id }}', '{{ task.task_id }}', 'dm.dm_f101_round_f', NOW(), 'RUNNING')
        RETURNING id INTO v_log_id;

        CALL dm.fill_f101_round_f('{{ params.report_date }}'::DATE);

        UPDATE logs.etl_log 
        SET end_time = NOW(), 
            status = 'SUCCESS' 
        WHERE id = v_log_id;

    EXCEPTION WHEN OTHERS THEN

        UPDATE logs.etl_log 
        SET end_time = NOW(), 
            status = 'ERROR', 
            error_message = SQLERRM 
        WHERE id = v_log_id;
        RAISE;
    END $$;
    """
)

task_export_csv = PythonOperator(
    task_id='export_f101_csv',
    python_callable=export_f101_to_csv,
)

(
    insert_ft_balance_f
    >> insert_ft_posting_f
    >> insert_md_account_d
    >> insert_md_currency_d
    >> insert_md_exchange_rate_d
    >> insert_md_ledger_account_s
    >> task_init_history
    >> task_turnover
    >> task_balance
    >> task_f101
    >> task_export_csv
)













