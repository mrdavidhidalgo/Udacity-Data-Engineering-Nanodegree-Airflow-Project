from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 aws_credentials="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_schema="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_schema = json_schema

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials)
        credentials_hook = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift_copy = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            JSON '{}'
         """
        s3_location = f"s3://{self.s3_bucket}/{self.s3_key}"
        self.log.info(f'The S3 location: {s3_location}') 
        sql = redshift_copy.format(
                self.table,
                s3_location,
                credentials_hook.access_key,
                credentials_hook.secret_key,
                self.json_schema
        )

        self.log.info(f'Executing SQL: {sql}')
        redshift_hook.run(sql)