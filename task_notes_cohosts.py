import json
import logging
import pandas as pd
from sqlalchemy import create_engine
from database_handling import DatabaseHandler
from google_api import Google

pd.options.mode.chained_assignment = None

logging.getLogger().setLevel(logging.INFO)


def add_merge_snippet(booking, merg, flat, google, internal_sheet_id, secrets, headers_rows: int = 3):
	# Compute the ROLLING offset, based on today - 15 - headers_row:
	offset_exact = google.excel_date(booking["reservation_start_adjusted"])
	offset_first = google.excel_date(pd.Timestamp.today() - pd.Timedelta(days=15))
	row = int(offset_exact - offset_first) + headers_rows  # Adjusting to the title rows where there's no date

	snippet = {
		"mergeCells": {
			"range": {
				"sheetId": internal_sheet_id,
				"startRowIndex": row - 1,
				"endRowIndex": row + int((booking["reservation_end"] - booking["reservation_start_adjusted"]).days) - 1,
				"startColumnIndex": google.col2num(secrets["flats"][flat]["reporting_col_owner"]),
				"endColumnIndex": google.col2num(secrets["flats"][flat]["reporting_col_owner"]) + 1
			},
			"mergeType": "MERGE_ALL"
		}
	}
	merg.append(snippet)


def add_notes_snippet(booking, notes, flat, google, internal_sheet_id, secrets, headers_rows: int = 3):
	# Compute the ROLLING offset, based on today - 15 - headers_row:
	offset_exact = google.excel_date(booking["reservation_start_adjusted"])
	offset_first = google.excel_date(pd.Timestamp.today() - pd.Timedelta(days=15))
	row = int(offset_exact - offset_first) + headers_rows  # Adjusting to the title rows where there's no date

	duration = (booking["reservation_end"] - booking["reservation_start"]).days
	note_body = f"""{booking["guest_name"].title()}\nZahlung: {booking["total_amount_paid_by_guest"]}€\nGäste: {booking["n_guests"]}\nNächte: {duration}\nAb {booking["reservation_start"].strftime("%d.%m")} Bis {booking["reservation_end"].strftime("%d.%m")}\nID: {booking["booking_id"]}"""

	snippet = {
		"updateCells": {
			"range": {
				"sheetId": internal_sheet_id,
				"startRowIndex": row - 1,
				"endRowIndex": row,
				"startColumnIndex": google.col2num(secrets["flats"][flat]["reporting_col_owner"]),
				"endColumnIndex": google.col2num(secrets["flats"][flat]["reporting_col_owner"]) + 1
			},
			"rows": [
				{
					"values": [
						{
							"note": note_body
						}
					]
				}
			],
			"fields": "note"
		}
	}
	notes.append(snippet)


def add_write_snippet(booking, data, flat, google, secrets):
	cell_range = google.get_rolling_range(unit_id=flat, date1=booking["reservation_start_adjusted"], headers_rows=3, col=secrets["flats"][flat]["reporting_col_owner"])
	part1 = booking["guest_name"].split(" ")[0].title()
	try:
		part2 = booking["guest_name"].split(" ")[1][0].title() + "."
	except IndexError as ie:
		part2 = ""
	shortened_name = f"""{part1} {part2}"""
	snippet = {
		"range": cell_range,
		"values": [
			[f"""{shortened_name} ({booking["platform"][0]})"""]
		]
	}
	data.append(snippet)


def update_notes_cohosts():
	"""
	1. Get all different clients
	2. For each client:
		2.1. Get workbook_id.
		2.2. For each flat of this client:
			2.2.1. Get bookings of this flat.
			2.2.2. Write them on the workbook.
	"""
	# 1. Get all workbooks / clients
	secrets = json.load(open('config_secrets.json'))
	db_engine = create_engine(url=secrets["database"]["url"])
	dbh = DatabaseHandler(db_engine, secrets)
	clients_wbids = list(set([secrets["flats"][flat]["reporting_workbook"] for flat in secrets["flats"] if "reporting_workbook" in secrets["flats"][flat]]))

	for wbid in clients_wbids:
		# 2. Get flats
		client_flats = [f for f in secrets["flats"] if ("reporting_workbook" in secrets["flats"][f]) and (secrets["flats"][f]["reporting_workbook"] == wbid)]
		sql = open("sql/task_notes_cohosts.sql").read()
		bookings = dbh.query_data(sql=sql, dtypes={"n_guests": int, "reservation_start_adjusted": pd.Timestamp, "reservation_start": pd.Timestamp, "reservation_end": pd.Timestamp})
		g = Google(secrets=secrets, workbook_id=wbid)

		# Clear workbook:
		g.write_note(0, 998, 0, 100, "", 920578163)
		g.unmerge_cells(0, 999, 0, 100, 920578163)

		dat = []
		notes = []
		merg = []

		for flat in client_flats:
			logging.info(f"Processing flat {flat}")
			logging.info(f"Cleared worksheet of values and notes.")

			b = bookings[bookings["object"] == flat]

			# Prepare the batchRequest: for each reservation end, create a batch snippet, append it to the data dict.
			b.apply(add_write_snippet, axis=1, args=(dat, flat, g, secrets))
			b.apply(add_notes_snippet, axis=1, args=(notes, flat, g, 920578163, secrets))
			b.apply(add_merge_snippet, axis=1, args=(merg, flat, g, 920578163, secrets))

		# Once you are done with the workbook, execute the batchRequest:
		# Write cell values
		g.batch_write_to_cell(data=dat)
		# Write notes
		g.batch_write_notes(requests=notes)
		# Merge booking cells
		g.batch_request(requests=merg)

		logging.info("Processed all notes for this flat.")

update_notes_cohosts()
