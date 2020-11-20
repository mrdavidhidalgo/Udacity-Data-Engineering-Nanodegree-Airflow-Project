from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift = PostgresHook(self.conn_id)

        for item in self.dq_checks:
            table_name = item.get('table')
            exp_result = item.get('expected_result')
            results = redshift.get_records(f"SELECT COUNT(1) FROM {table_name}")
            if len(results) < 1 or len(results[0]) < 1:
                raise ValueError(f"Data quality check failed. {table_name} returned no results")
            num_records = results[0][0]
            if num_records < exp_result:
                raise ValueError(f"Data quality check failed. {table_name} contained less than {exp_result} rows")
            self.log.info(f"Data quality on table {table_name} check passed with {results[0][0]} records")

        self.log.info("The data quality passed")