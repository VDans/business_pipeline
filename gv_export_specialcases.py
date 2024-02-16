import json
import logging
import pandas as pd
from sqlalchemy import create_engine
from database_handling import DatabaseHandler

secrets = json.load(open('config_secrets.json'))
logging.getLogger().setLevel(logging.INFO)
db_engine = create_engine(url=secrets["database"]["url"])
dbh = DatabaseHandler(db_engine, secrets)

OUTPUT_PATH = "/Users/valentindans/Library/CloudStorage/GoogleDrive-office@host-it.at/My Drive/Legal/Gästeverzeichnis"

# Special cases include: GBS, LORY, RHG
flats = [f[0] for f in secrets["flats"].items() if f[1]["pricing_col"] != ""]
date_from = pd.Timestamp(day=1, month=1, year=2024)
date_to = pd.Timestamp(day=31, month=1, year=2024)

sql = open("sql/gv_preparation.sql").read()
gv_table = dbh.query_data(sql=sql.format(f"'RHG15', 'RHG20', 'RHG30', 'RHG32'", f"'{date_from.strftime('%Y-%m-%d')}'", f"'{date_to.strftime('%Y-%m-%d')}'"))
gv_table.to_csv(path_or_buf=f"{OUTPUT_PATH}/{date_to.strftime('%Y-%m')}/{date_to.strftime('%Y%m')}_RHG_all.csv",
                sep=";",
                index=False)
