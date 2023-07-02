import sqlalchemy as db
from sqlalchemy import text
import pandas as pd
from io import StringIO

#desde utilidades trae las funciones que hemos creado
from utils.Utilities import Utilities as u

from pandas import DataFrame

class Extract():
    def __ini__(self) -> None:
        self.process = "Extractprocess"

    def read_mysql(self,db_name, table_name):
        engine = db.create_engine(f"mysql://root:root@192.168.1.50:3310/{db_name}")
        conn = engine.connect()
        df = pd.read_sql_query(text(f'SELECT * FROM {table_name}'), con=conn)
        return df

    def read_gcp(self, _bucketname, _filename):
        client = u.cliente_gcp()
        bucket = client.get_bucket(f'{_bucketname}')
        blob = bucket.get_blob(f'{_filename}')
        downloaded_file = blob.download_as_text(encoding="utf-8")
        df = pd.read_csv(StringIO(downloaded_file))
        return df