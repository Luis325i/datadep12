from io import StringIO
import io
import pandas as pd
import os
from io import StringIO

from utils.Utilities import Utilities as u

class Load():
    def __ini__(self) -> None:
        self.process = "Loadprocess"

    def load_to_gcloud(self, _bucketName, _fileName, _df):
        client = u.cliente_gcp()
        bucket = client.get_bucket(f'{_bucketName}')
        bucket.blob(_fileName).upload_from_string(_df.to_csv(encoding = "utf-8",index=False), 'text/csv')

    def load_to_mysql(self, _dbname, _tablename, _df):
        conn = u.cliente_mysql(_dbname)
        _df.to_sql(con=conn, name= _tablename, if_exists='replace',index=False)
        return None

