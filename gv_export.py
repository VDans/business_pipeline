import json
import logging
import pandas as pd
from sqlalchemy import create_engine
from database_handling import DatabaseHandler

secrets = json.load(open('config_secrets.json'))
logging.getLogger().setLevel(logging.INFO)
db_engine = create_engine(url=secrets["database"]["url"])
dbh = DatabaseHandler(db_engine, secrets)

OUTPUT_PATH = "G:/My Drive/Legal/Gästeverzeichnis"


flats = [f[0] for f in secrets["flats"].items() if f[1]["pricing_col"] != ""]
date_from = pd.Timestamp(day=1, month=8, year=2023)
date_to = pd.Timestamp(day=31, month=8, year=2023)

for f in flats:
    sql = open("sql/gv_preparation.sql").read()
    gv_table = dbh.query_data(sql=sql.format(f"'{f}'", f"'{date_from.strftime('%Y-%m-%d')}'", f"'{date_to.strftime('%Y-%m-%d')}'"))
    gv_table.to_csv(path_or_buf=f"{OUTPUT_PATH}/{f}.csv",
                    sep=";",
                    index=False)
