from datetime import datetime, timedelta
import pendulum
import os

import sql_queries

from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator



default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    default_args=default_args
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket="sparkify-a02fe025-cd6c-4261-87f6-e3fee41dde71-dl",
        s3_key="log_data/{execution_date.year}/{execution_date.month}/"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="sparkify-a02fe025-cd6c-4261-87f6-e3fee41dde71-dl",
        s3_key="song_data/A/A/"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        table="songplays",
        sql_query=sql_queries.SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table="user",
        sql_query=sql_queries.SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table="song",
        sql_query=sql_queries.SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table="artist",
        sql_query=sql_queries.SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table="time",
        sql_query=sql_queries.SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        test_cases=[
            {
                "test_sql": "SELECT COUNT(*) FROM user WHERE user_id IS NULL;",
                "expected_result": 0
            },
            {
                "test_sql": "SELECT COUNT(*) FROM song WHERE song_id IS NULL;",
                "expected_result": 0
            },
            {
                "test_sql": "SELECT COUNT(*) FROM artist WHERE song_id IS NULL;",
                "expected_result": 0
            },
            {
                "test_sql": "SELECT COUNT(*) FROM time WHERE song_id IS NULL;",
                "expected_result": 0
            }
        ],
        tables=["songplays", "user", "song", "artist", "time"]
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

    load_songplays_table << [stage_songs_to_redshift, stage_events_to_redshift]

    load_songplays_table >> [load_song_dimension_table, load_user_dimension_table,
                             load_artist_dimension_table, load_time_dimension_table]

    run_quality_checks << [load_song_dimension_table, load_user_dimension_table,
                           load_artist_dimension_table, load_time_dimension_table]

    run_quality_checks >> end_operator


final_project_dag = final_project()
