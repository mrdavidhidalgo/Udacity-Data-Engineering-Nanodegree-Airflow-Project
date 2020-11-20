from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'DavidH',
    'start_date': datetime(2020, 11, 18),
    'depends_on_past': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email': ['email@spfy.com'],
    'retries': 3
}

dag = DAG('Load_data_task',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="stage_events",
    conn_id="redshift",
    aws_credentials="aws_credentials",
    table="staging_events",
    s3_bucket='udacity-dend',
    s3_key="log_data",
    json_schema="s3://udacity-dend/log_json_path.json",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    conn_id="redshift",
    aws_credentials="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song-data",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    conn_id="redshift",
    table="songplays",
    select_statement = SqlQueries.songplay_table_insert,
    delete_data=True,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    conn_id="redshift",
    table="users",
    select_statement = SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    conn_id="redshift",
    table="songs",
    select_statement = SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    conn_id="redshift",
    table="artists",
    select_statement = SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    conn_id="redshift",
    table="time",
    select_statement = SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    conn_id="redshift",
    dq_checks=[{"table":"songplays" , "expected_result":"10"}, {"table":"users" , "expected_result":"10"}, {"table":"artists" , "expected_result":"10"}],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift,stage_songs_to_redshift] >>load_songplays_table
load_songplays_table >> [load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator