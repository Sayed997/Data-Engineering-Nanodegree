from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#This operator extracts data from S3 and stages them in Redshift
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 ignore_headers=1,
                 json_path="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ignore_headers = ignore_headers
        self.json_path = json_path

    def execute(self, context):
        self.log.info(' Creating Staging Tables in Redshift from S3')
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        
        redshift.run(f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{credentials.access_key}' \
                     SECRET_ACCESS_KEY '{credentials.secret_key}' FORMAT AS JSON '{self.json_path}' COMPUPDATE OFF")
        
        





