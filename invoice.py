from flask import Flask, render_template, send_file
from datetime import datetime
from weasyprint import HTML
import json
import logging

import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, update
from database_handling import DatabaseHandler
from zodomus_api import Zodomus

# secrets = json.load(open('config_secrets.json'))
# db_engine = create_engine(url=secrets["database"]["url"])


class Invoice:
    def __init__(self, secrets, db_engine):
        self.secrets = secrets
        self.db_engine = db_engine

        self.dbh = DatabaseHandler(db_engine, secrets)

    def generate_invoice(self):
        """
        Generate the invoice html with the variables.
        """
        # --snip-- #
        rendered = render_template('invoice.html',
                                   invoice_id=invoice_id,
                                   invoice_date=invoice_date,

                                   guest_name=guest_name,
                                   guest_company=guest_company,
                                   guest_address=guest_address,
                                   guest_email=guest_email,
                                   guest_phone=guest_phone,

                                   rs_first_name=rs_first_name,
                                   rs_last_name=rs_last_name,
                                   rs_company_name=rs_company_name,
                                   rs_street=rs_street,
                                   rs_street_number=rs_street_number,
                                   rs_postal_code=rs_postal_code,
                                   rs_city=rs_city,
                                   rs_vat_number=rs_vat_number
                                   # rs_email=rs_email,
                                   # rs_phone=rs_phone,
                                   # rs_company_registry_number=rs_company_registry_number

                                   )
        html = HTML(string=rendered)
        rendered_pdf = html.write_pdf('invoice.pdf')

        # return send_file(
        #     'invoice.pdf'
        # )