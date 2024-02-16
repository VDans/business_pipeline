import json
import pandas as pd
from sqlalchemy import create_engine, types

secrets = json.load(open('config_secrets.json'))
db_engine = create_engine(url=secrets["database"]["url"])

# Preparing the initial pricing table
# Read original from Pricing sheet 1.0:
df = pd.read_excel("/Users/valentindans/Downloads/pricing_data.xlsx", header=None, index_col=None)
df = df.fillna(0)
df = df.T.reset_index()
df1 = pd.melt(frame=df, id_vars=["level_0", "level_1"], var_name="price_date")
df2 = df1.pivot(index=["price_date", "level_0"], columns='level_1', values="value")
df2 = df2.reset_index()

# Rename correctly:
df2 = df2.rename(columns={"level_0": "object", "Min.": "min_nights", "Price": "price"})
df2["change_date"] = pd.Timestamp.now()
df2["overwritten"] = False

# Correct data
# Convert dates
df2['price_date'] = [pd.to_datetime(d).date() for d in df2['price_date']]

# Non-numerical to 0 & protect prices in closed nights:
df2['min_nights'] = pd.to_numeric(df2['min_nights'], errors='coerce').fillna(0)
df2['min_nights'] = [int(m) for m in df2['min_nights']]
df2.loc[df2['min_nights'] == 0, "price"] = 1000

with db_engine.begin() as conn:
    df2.to_sql(
        index=None,
        con=conn,
        name='pricing',
        if_exists='append'
    )
