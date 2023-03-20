from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        table="",
        sql_query="",
        mode=""
        *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.mode = mode


    def execute(self, context):
        self.log.info(f"Loading dimension table '{self.table}'")
        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        if self.mode not in ("append-only", "delete-load"):
            raise ValueError(f"Parameter mode expects 'append-only' or 'delete-load', but {self.mode} was passed.")
        
        if self.mode == "delete-load":
            redshift.run(f"DELETE FROM {self.table}")
        
        redshift.run(f"INSERT INTO {self.table} {self.sql_query}")
