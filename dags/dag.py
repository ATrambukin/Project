import pandas as pd
import numpy as np
import time

from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
        df = pd.read_csv(f'/Файлы проекта/{table}.csv', dtype=str, encoding='utf-8-sig', encoding_errors='replace', sep=';')
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


insert_ft_balance_f >> insert_ft_posting_f >> insert_md_account_d >> insert_md_currency_d >> insert_md_exchange_rate_d >> insert_md_ledger_account_s














