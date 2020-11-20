from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                conn_id="",
                table="",
                select_statement="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.select_statement = select_statement

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)

        self.log.info("Clearing data from destination Redshift table")
        hook.run("DELETE FROM {}".format(self.table))

       
        select_to_insert_sql = """
        INSERT INTO {}
        {}
        """
        sql = select_to_insert_sql.format(self.table, self.select_statement)
        self.log.info(f"Executed  SQL {sql} ...")
        hook.run(sql)
