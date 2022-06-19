from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (StageToRedshiftOperator, CopyToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner' : 'awsuser',
    'start_date' : datetime(2022, 6, 19),
    'depends_on_past' : False,
    'retries ': 3,
    'retry_delay' : timedelta(minutes = 5),
    'catchup' : False
}

dag = DAG('udac_example_dag',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
#           schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id = 'Begin_execution',  dag = dag)

stage_immigration_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_immigration',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'capstone-project-mt',
    s3_key = 'immigration-data',    
    table_name = 'staging_immigration',
    format_enriched = 'Parquet'
)

copy_airport_code_csv_to_redshift = CopyToRedshiftOperator(
    task_id = 'copy_airport_code_csv',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'capstone-project-mt',
    s3_key = 'airport-codes_csv.csv',    
    table_name = 'airport_code',
    format_enriched = 'CSV IGNOREHEADER 1',
    region = 'us-west-2'
)

copy_country_code_csv_to_redshift = CopyToRedshiftOperator(
    task_id = 'copy_country_code_csv',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'capstone-project-mt',
    s3_key = 'i94cntyl.csv',    
    table_name = 'country_codes',
    format_enriched = 'CSV IGNOREHEADER 1',
    region = 'us-west-2'
)

copy_airport_city_csv_to_redshift = CopyToRedshiftOperator(
    task_id = 'copy_airport_city_csv',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'capstone-project-mt',
    s3_key = 'i94prtl.csv',    
    table_name = 'airport_city',
    format_enriched = 'CSV IGNOREHEADER 1',
    region = 'us-west-2'
)

copy_airport_type_csv_to_redshift = CopyToRedshiftOperator(
    task_id = 'copy_airport_type_csv',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'capstone-project-mt',
    s3_key = 'i94model.csv',    
    table_name = 'airport_type',
    format_enriched = 'CSV IGNOREHEADER 1',
    region = 'us-west-2'
)

copy_state_csv_to_redshift = CopyToRedshiftOperator(
    task_id = 'copy_state_csv',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'capstone-project-mt',
    s3_key = 'i94addrl.csv',    
    table_name = 'states',
    format_enriched = 'CSV IGNOREHEADER 1',
    region = 'us-west-2'
)

copy_visa_type_csv_to_redshift = CopyToRedshiftOperator(
    task_id = 'copy_visa_type_csv',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'capstone-project-mt',
    s3_key = 'i94visa.csv',    
    table_name = 'visa_type',
    format_enriched = 'CSV IGNOREHEADER 1',
    region = 'us-west-2'
)

load_immigration_fact_table = LoadFactOperator(
    task_id = 'load_immigration_fact_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table_name = 'immigration',
    sql = SqlQueries.immigration_table_insert
)

load_passenger_dimension_table = LoadDimensionOperator(
    task_id = 'load_passenger_dimension_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table_name = 'passenger',
    sql = SqlQueries.passenger_table_insert
)

load_flight_dimension_table = LoadDimensionOperator(
    task_id = 'load_flight_dimension_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table_name = 'flights',
    sql = SqlQueries.flight_table_insert
)

load_flight_flag_dimension_table = LoadDimensionOperator(
    task_id = 'load_flight_flag_dimension_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table_name = 'flight_flags',
    sql = SqlQueries.flight_flag_table_insert
)

load_visa_dimension_table = LoadDimensionOperator(
    task_id = 'load_visa_dimension_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table_name = 'visas',
    sql = SqlQueries.visa_table_insert
)

DQ_COUNT_DICT = [
    {'query' : 'SELECT COUNT(*) FROM public.immigration', 'expected_outcome' : 3096313},
    {'query' : 'SELECT COUNT(*) FROM public.passenger', 'expected_outcome' : 3096313},
    {'query' : 'SELECT COUNT(*) FROM public.flights', 'expected_outcome' : 3096313},
    {'query' : 'SELECT COUNT(*) FROM public.flight_flags', 'expected_outcome' : 3096313},
    {'query' : 'SELECT COUNT(*) FROM public.visas', 'expected_outcome' : 3096313},
    {'query' : 'SELECT COUNT(*) FROM public.airport_code', 'expected_outcome' : 55075},
    {'query' : 'SELECT COUNT(*) FROM public.country_codes', 'expected_outcome' : 289},
    {'query' : 'SELECT COUNT(*) FROM public.airport_city', 'expected_outcome' : 660},
    {'query' : 'SELECT COUNT(*) FROM public.airport_type', 'expected_outcome' : 4},
    {'query' : 'SELECT COUNT(*) FROM public.states', 'expected_outcome' : 55},
    {'query' : 'SELECT COUNT(*) FROM public.visa_type', 'expected_outcome' : 3}
]

DQ_PK_UNIQUE_STATEMENT = '''SELECT {}, COUNT(*) FROM public.{} GROUP BY {} HAVING COUNT(*) > 1'''

DQ_PK_UNIQUE_DICT = [
    {'query' : DQ_PK_UNIQUE_STATEMENT.format('admission_no', 'immigration', 'admission_no'), 'expected_outcome' : 0},
    {'query' : DQ_PK_UNIQUE_STATEMENT.format('passenger_id', 'passenger', 'passenger_id'), 'expected_outcome' : 0},
    {'query' : DQ_PK_UNIQUE_STATEMENT.format('flight_id', 'flights', 'flight_id'), 'expected_outcome' : 0},
    {'query' : DQ_PK_UNIQUE_STATEMENT.format('cic_id', 'flight_flags', 'cic_id'), 'expected_outcome' : 0},
    {'query' : DQ_PK_UNIQUE_STATEMENT.format('visa_id', 'visas', 'visa_id'), 'expected_outcome' : 0},
    {'query' : DQ_PK_UNIQUE_STATEMENT.format('ident', 'airport_code', 'ident'), 'expected_outcome' : 0}
]

run_data_quality_count_checks = DataQualityOperator(
    task_id = 'run_data_quality_count_checks',
    dag = dag,
    redshift_conn_id = 'redshift',
    tables_dict = DQ_COUNT_DICT
)

run_data_quality_pk_unique_checks = DataQualityOperator(
    task_id = 'run_data_quality_pk_unique_checks',
    dag = dag,
    redshift_conn_id = 'redshift',
    tables_dict = DQ_COUNT_DICT
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_immigration_to_redshift, copy_airport_code_csv_to_redshift, copy_country_code_csv_to_redshift, copy_airport_city_csv_to_redshift, copy_airport_type_csv_to_redshift, copy_state_csv_to_redshift, copy_visa_type_csv_to_redshift] >> load_immigration_fact_table >> [load_passenger_dimension_table, load_flight_dimension_table, load_flight_flag_dimension_table, load_visa_dimension_table] >> run_data_quality_count_checks >> run_data_quality_pk_unique_checks >> end_operator