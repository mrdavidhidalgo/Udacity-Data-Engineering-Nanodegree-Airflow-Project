from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
        Execute a insert from a select
    """

    ui_color = '#80BD9E'

    
    @apply_defaults
    def __init__(self,
                conn_id="",
                table="",
                clean_before = False,
                select_statement="",
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.clean_before = clean_before
        self.select_statement = select_statement

    def execute(self, context):
        db_hook = PostgresHook(postgres_conn_id=self.conn_id)

        if self.clean_before:
            self.log.info("Deleting table {}".format(self.table))
            db_hook.run("DELETE FROM {}".format(self.table))

        
        select_to_insert_sql = """
        INSERT INTO {}
          {}
         """
        sql = select_to_insert_sql.format(self.table, self.select_statement)
        self.log.info(f"Executing SQL -> {sql} ...")
        db_hook.run(sql)
