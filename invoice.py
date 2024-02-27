from flask import Flask, render_template, send_file
from datetime import datetime
from weasyprint import HTML
import json
import logging

import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, update
from database_handling import DatabaseHandler


class Invoice:
    def __init__(self, secrets, db_engine):
        self.secrets = secrets
        self.db_engine = db_engine

        self.dbh = DatabaseHandler(db_engine, secrets)

    def generate_invoice(self, data: dict | pd.DataFrame):
        """
        Generate the invoice html with the variables.
        """
        rendered = render_template('invoice.html',
                                   invoice_id=data["invoice_id"],
                                   invoice_date=data["invoice_date"],

                                   guest_name=data["guest_name"],
                                   guest_company=data["guest_company"],
                                   guest_address=data["guest_address"],
                                   guest_email=data["guest_email"],
                                   guest_phone=data["guest_phone"],

                                   rs_first_name=data["rs_first_name"],
                                   rs_last_name=data["rs_last_name"],
                                   rs_company_name=data["rs_company_name"],
                                   rs_street=data["rs_street"],
                                   rs_street_number=data["rs_street_number"],
                                   rs_postal_code=data["rs_postal_code"],
                                   rs_city=data["rs_city"],
                                   rs_vat_number=data["rs_vat_number"],
                                   # rs_email=data["rs_email"],
                                   # rs_phone=data["rs_phone"],
                                   # rs_company_registry_number=data["rs_company_registry_number"],

                                   )
        html = HTML(string=rendered)
        rendered_pdf = html.write_pdf('invoice.pdf')
