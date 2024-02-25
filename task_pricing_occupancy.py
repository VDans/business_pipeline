import json
import logging
import pandas as pd
import logging
import itertools as it
import json
from sqlalchemy import create_engine
from database_handling import DatabaseHandler
from google_api import Google

logging.getLogger().setLevel(logging.INFO)
pd.options.mode.chained_assignment = None

secrets = json.load(open('config_secrets.json'))
db_engine = create_engine(url=secrets["database"]["url"])
dbh = DatabaseHandler(db_engine, secrets)
g = Google(secrets, secrets["google"]["pricing_workbook_id_horizontal"])


def compute_occupancy():
    """
    The goal here is to give general occupancy of the flats available on the pricing sheet, per day (not per flat!).

    1/ Pull all bookings with OK status from 15 days ago to 1 year in advance.
    2/ Pull and filter all available flats' names.
    3/ For each booking, create a list of days between start and end, and associate it with the flat name.
    4/ For each date between 15 days ago and 1 year from now, join the date with each flat's booked dates.
    5/ Replace all nulls with 0, and filled cells with 1.
    6/ Compute occupancy per day.
    7/ Write the occupancy on the Pricing Sheet
    """
    logging.info(f"The time right now is: {pd.Timestamp.now()}")
    logging.info(f"Starting the task to compute occupancy per day.")

    # 1/
    sql = open("sql/task_occupancy.sql").read()
    bookings = dbh.query_data(sql=sql, dtypes={"reservation_start": pd.Timestamp, "reservation_end": pd.Timestamp})

    # 2/ Filters
    flats = [f[0] for f in secrets["flats"].items() if "pricing_row" in f[1]]
    bookings = bookings[bookings["object"].isin(flats)]
    all_dates = list(pd.date_range(start=(pd.Timestamp.today().date() - pd.Timedelta(days=9)), end=(pd.Timestamp.today().date() + pd.Timedelta(days=180))))

    # 3/
    # To be vectorized!
    df_date_ranges = pd.DataFrame(columns=["object", "date"])
    for i in range(len(bookings)):
        a = bookings.iloc[i]
        date_range = list(pd.date_range(start=a['reservation_start'], end=a['reservation_end']))

        df_date_ranges_add = pd.DataFrame({"object": a["object"], "date": date_range, "occupancy": 1})
        df_date_ranges = pd.concat([df_date_ranges, df_date_ranges_add], ignore_index=True)

    # df_date_ranges = df_date_ranges.drop_duplicates()

    # Output Goal
    index_mix = list(it.product(flats, all_dates))
    out = pd.DataFrame(index_mix).rename(columns={0: "object", 1: "date"})
    # Join them:
    out = pd.merge(out, df_date_ranges, on=["object", "date"], how="left").fillna(0)
    # Pivot from long to wide:
    out = pd.pivot(out, index="date", columns="object", values="occupancy")
    out["occupancy"] = out.sum(axis=1) / len(flats)
    out = out.reset_index()

    # Write cell values
    dat = []
    out.apply(add_write_snippet, axis=1, args=(dat, ))
    g.batch_write_to_cell(data=dat)


def add_write_snippet(row_dat, data):
    # Calculate the A1 notation of where the name of the booking should be.
    # In this new concept, the name should expand on two rows.
    target_col = g.get_rolling_col(date1=row_dat["date"], today_col="L")
    cell_range = target_col + "2"
    snippet = {
        "range": cell_range,
        "values": [
            [str(round(100 * row_dat["occupancy"], 1)) + "%"]
        ]
    }
    data.append(snippet)


def add_write_snippet1(row_dat, data, headers_rows = 3):
    offset_exact = g.excel_date(row_dat["date"])
    offset_first = g.excel_date(pd.Timestamp.today() - pd.Timedelta(days=15))
    row = int(offset_exact - offset_first) + headers_rows  # Adjusting to the title rows where there's no date

    cell_range = "B" + str(row)

    snippet = {
        "range": cell_range,
        "values": [
            [str(round(100 * row_dat["occupancy"], 1)) + "%"]
        ]
    }
    data.append(snippet)


compute_occupancy()
