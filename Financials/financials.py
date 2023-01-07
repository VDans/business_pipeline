# import pandas as pd
#
#
# class Financials:
#     def __init__(self, unit_id, db_connection):
#         self.unit_id = unit_id
#         self.db_connection = db_connection
#
#         self.inputs = None
#         self.kpis = {}
#
#         self.get_inputs()
#         self.compute_variables()
#
#     def get_inputs(self):
#         self.inputs = pd.read_sql(sql=f"SELECT * FROM BOOKINGS WHERE unit_id = '{self.unit_id}'",
#                                   con=self.db_connection,
#                                   index_col=None)
#
#     def compute_variables(self):
#         self.inputs["nights"] = (self.inputs["check_out"] - self.inputs["check_in"]).dt.days
#         self.inputs["net_revenue"] = self.inputs["price"] - self.inputs["payment_commission"] - self.inputs["platform_commission"]
#         self.inputs["night_gross_revenue"] = self.inputs["price"] / self.inputs["nights"]
#         self.inputs["night_net_revenue"] = self.inputs["net_revenue"] / self.inputs["nights"]
#
#     def compute_kpis(self, from_date, to_date):
#
#         # Filter according to the parameters:
#         self.inputs = self.inputs[self.inputs["check_in"] >= from_date]
#         self.inputs = self.inputs[self.inputs["check_in"] <= to_date]
#
#         # Remove canceled reservations:
#         self.inputs = self.inputs[self.inputs['price'] > 0]
#
#         # Compute interesting KPIs: Tuples containing the "nice" phrasing for displaying:
#         self.kpis["gross_revenue"] = {
#             "display_name": "Gross Revenue",
#             "value": self.inputs['price'].sum(),
#             "units": "EUR",
#             "cleaning_function": lambda x: round(x, 2)
#         }
#         self.kpis["net_revenue"] = {
#             "display_name": "Net Revenue",
#             "value": self.inputs['net_revenue'].sum(),
#             "units": "EUR",
#             "cleaning_function": lambda x: round(x, 2)
#         }
#         self.kpis["nights_booked"] = {
#             "display_name": "Nights Booked",
#             "value": self.inputs["nights"].sum(),
#             "units": "Nights",
#             "cleaning_function": lambda x: x
#         }
#         self.kpis["nights_available"] = {
#             "display_name": "Nights Available",
#             "value": ((to_date - from_date).days + 1),
#             "units": "Nights",
#             "cleaning_function": lambda x: x
#         }
#         self.kpis["nights_free"] = {
#             "display_name": "Nights Free",
#             "value": self.kpis["nights_available"]["value"] - self.kpis["nights_booked"]["value"],
#             "units": "Nights",
#             "cleaning_function": lambda x: x
#         }
#         self.kpis["occupancy"] = {
#             "display_name": "Occupancy",
#             "value": self.kpis["nights_booked"]["value"] / self.kpis["nights_available"]["value"],
#             "units": "%",
#             "cleaning_function": lambda x: round(100 * x, 2)
#         }
#         self.kpis["average_night_gross_revenue"] = {
#             "display_name": "Avg GR / Night",
#             "value": self.kpis["gross_revenue"]["value"] / self.kpis["nights_booked"]["value"],
#             "units": "EUR",
#             "cleaning_function": lambda x: round(x, 2)
#         }
#         self.kpis["average_night_net_revenue"] = {
#             "display_name": "Avg NR / Night",
#             "value": self.kpis["net_revenue"]["value"] / self.kpis["nights_booked"]["value"],
#             "units": "EUR",
#             "cleaning_function": lambda x: round(x, 2)
#         }
