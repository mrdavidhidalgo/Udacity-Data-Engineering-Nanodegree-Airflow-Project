from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

import sql_tables

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

dag = DAG('create_table_task',
           description ='Create tables in RedShift',
           start_date = datetime(2020, 11, 19),
           schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_table_artists_task = PostgresOperator(
    task_id='create_artists_table',
    postgres_conn_id="redshift",
    sql=sql_tables.CREATE_ARTISTS,
    dag=dag
)

create_table_songplays_task = PostgresOperator(
    task_id='create_sonplays_table',
    postgres_conn_id="redshift",
    sql=sql_tables.CREATE_SONPLAYS,
    dag=dag
)

create_table_song_task = PostgresOperator(
    task_id='create_songs_table',
    postgres_conn_id="redshift",
    sql=sql_tables.CREATE_SONGS,
    dag=dag
)

create_table_staging_events_task = PostgresOperator(
    task_id='create_staging_events_table',
    postgres_conn_id="redshift",
    sql=sql_tables.CREATE_STAGING_EVENTS,
    dag=dag
)

create_table_staging_songs_task = PostgresOperator(
    task_id='create_staging_songs_table',
    postgres_conn_id="redshift",
    sql=sql_tables.CREATE_STAGING_SONGS,
    dag=dag
)

create_table_time_task = PostgresOperator(
    task_id='create_time_table',
    postgres_conn_id="redshift",
    sql=sql_tables.CREATE_TIME,
    dag=dag
)

create_table_users_task = PostgresOperator(
    task_id='create_users_table',
    postgres_conn_id="redshift",
    sql=sql_tables.CREATE_USERS,
    dag=dag
) 

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >>  create_table_artists_task
start_operator >>  create_table_users_task
start_operator >>  create_table_time_task
start_operator >>  create_table_staging_songs_task
start_operator >>  create_table_staging_events_task
start_operator >>  create_table_song_task
start_operator >>  create_table_songplays_task
create_table_artists_task >> end_operator
create_table_users_task >> end_operator
create_table_time_task >> end_operator
create_table_staging_songs_task >> end_operator
create_table_staging_events_task >> end_operator
create_table_song_task >> end_operator
create_table_songplays_task >> end_operator
