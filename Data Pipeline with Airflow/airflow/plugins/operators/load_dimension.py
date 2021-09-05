from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.append = append

    def execute(self, context):
        self.log.info(f"In process of loading table {self.table}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append:
            redshift.run(f"INSERT INTO {self.table} {self.sql_query}")
            
        else:
            redshift.run(f"TRUNCATE TABLE {self.table}")
            redshift.run(f"INSERT INTO {self.table} {self.sql_query}")
        
        
