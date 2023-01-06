import pandas as pd
from typing import AnyStr
import logging
import numpy as np
from time import time


class DatabaseHandler:

    def __init__(self, db_connection=None):

        self.db_connection = db_connection

        self.curs = self.db_connection.cursor()
        self.curs.prefetchrows = 0
        self.curs.arraysize = 1000000

    def query_gdwh_data(self, sql: AnyStr, data=None, dtypes=None):

        self.curs.execute(sql,  data)
        col_names = [i[0].lower() for i in self.curs.description]
        df = self.curs.fetchmany()

        df = pd.DataFrame(data=df,
                          columns=col_names)

        if dtypes:
            df = self.force_dtypes(df, dtypes)

        return df

    def upload_sql(self, df, sql, chunksize: int = 10000):

        if 'datum' in df.columns:
            df['datum'] = [d.strftime('%Y-%m-%d') for d in df['datum']]

        start = time()
        if len(df) > chunksize:
            n_chunks = (len(df) // chunksize) + 1
            dfs_list = np.array_split(df, n_chunks)
            logging.info('Number of chunks: ' + str(n_chunks))

            for d in dfs_list:
                df_temp = list(d.to_records(index=False))
                self.curs.executemany(sql, df_temp)

        else:
            df = list(df.to_records(index=False))
            self.curs.executemany(sql, df)
        end = time()

        logging.info('The data were succesfully uploaded in ' + str(round(end-start, 0)) + ' seconds')

    @staticmethod
    def force_dtypes(df: pd.DataFrame, dtypes: dict = None):
        # Find the keys in dtypes where the value is "Timestamp" or "datetime", and convert those, while removing them from the dtypes dict, to transmit to astype()
        date_type_keys: list = [k for k, v in dtypes.items() if "Timestamp" in str(v) or "datetime" in str(v)]  # Einstein?
        for c in date_type_keys:
            df[c] = pd.to_datetime(df[c])
            dtypes.pop(c)

        # Convert now the rest
        df = df.astype(dtypes)

        return df
