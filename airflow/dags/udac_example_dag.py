from datetime import datetime, timedelta
import os
from airflow import DAG
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator

from helpers import SqlQueries
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner' : 'awsuser',
    'start_date' : datetime(2022, 6, 10),
    'depends_on_past' : False,
    'retries ': 3,
    'retry_delay' : timedelta(minutes = 5),
    'catchup' : False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
#           schedule_interval='@daily',
          max_active_runs = 1
         )

start_operator = DummyOperator(task_id = 'begin_execution',  dag=dag)

# create_tables = PostgresOperator(
#     task_id = 'create_tables',
#     dag = dag,
#     sql = 'create_tables.sql',
#     postgres_conn_id = 'redshift'
# )

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_events',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data',    
    table_name = 'staging_events',
#     format_ = 'JSON',
    format_as = 's3://udacity-dend/log_json_path.json',
    region = 'us-west-2'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_songs',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data',    
    table_name = 'staging_songs',
#     format_ = 'JSON',
    format_as = 'auto',
    region = 'us-west-2'
)

load_songplays_table = LoadFactOperator(
    task_id = 'load_songplays_fact_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table_name = 'songplays',
    sql = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'load_user_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table_name = 'users',
    sql = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'load_song_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table_name = 'songs',
    sql = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'load_artist_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table_name = 'artists',
    sql = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'load_time_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table_name = 'time',
    sql = SqlQueries.time_table_insert
)

DQ_DICT = [
    {'query' : 'SELECT COUNT(*) FROM public.artists', 'expected_outcome' : 10025},
    {'query' : 'SELECT COUNT(*) FROM public.time', 'expected_outcome' : 6820},
    {'query' : 'SELECT COUNT(*) FROM public.users', 'expected_outcome' : 104},
    {'query' : 'SELECT COUNT(*) FROM public.songs', 'expected_outcome' : 14896},
    {'query' : 'SELECT COUNT(*) FROM public.songplays', 'expected_outcome' : 6820}
]

run_quality_checks = DataQualityOperator(
    task_id = 'run_data_quality_checks',
    dag = dag,
    redshift_conn_id = 'redshift',
    tables_dict = DQ_DICT
)

end_operator = DummyOperator(task_id = 'stop_execution',  dag = dag)

# dependencies
start_operator >> [stage_songs_to_redshift, stage_events_to_redshift] >> load_songplays_table >> [load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table, load_user_dimension_table] >> run_quality_checks >> end_operator
