from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#Ensures Data is correctly loaded into specified tables
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[], 
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('Data Quality checks in process')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for df in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {df}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {df} returned no records")   
            if records[0][0] < 1:
                 raise ValueError(f"Data quality checks failed: {df} contained no rows")
            self.log.info(f"Data Quality checks for {df} passed with {records[0][0]} records")                       