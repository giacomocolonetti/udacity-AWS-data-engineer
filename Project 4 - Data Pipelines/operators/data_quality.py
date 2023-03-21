from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        tables=[],
        test_cases=[],
        *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.tables = tables
        self.test_cases = test_cases


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        for test_case in self.test_cases:
            test_sql = test_case.get("test_sql")
            expected_result = test_case.get("expected_result")

            self.log.info(f'Executing data quality check: {test_sql}')
            records = redshift.get_records(test_sql)

            if records[0][0] != expected_result:
                raise ValueError(f"Data quality check failed. Test: {test_sql}. Expected result: {expected_result}. Actual result: {records[0][0]}")
            else:
                self.log.info(f"Data quality check passed. Test: {test_sql}. Expected result: {expected_result}. Actual result: {records[0][0]}")

        for table in self.tables:
            self.log.info(f'Checking {table} data quality')
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table} LIMIT 1")

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {self.table} returned no results")
            num_records = records[0][0]

            if num_records < 1:
                raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")

            self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")
