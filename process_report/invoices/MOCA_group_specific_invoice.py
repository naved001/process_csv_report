import os
import sys
from dataclasses import dataclass
import subprocess
import tempfile
import logging

import pandas
from jinja2 import Environment, FileSystemLoader

import process_report.invoices.invoice as invoice
import process_report.util as util


TEMPLATE_DIR_PATH = "process_report/templates"


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class MOCAGroupInvoice(invoice.Invoice):
    CREDIT_COLUMN_COPY_LIST = [
        invoice.INVOICE_DATE_FIELD,
        invoice.INVOICE_EMAIL_FIELD,
        invoice.GROUP_NAME_FIELD,
        invoice.GROUP_INSTITUTION_FIELD,
    ]
    TOTAL_COLUMN_LIST = [
        invoice.COST_FIELD,
        invoice.GROUP_BALANCE_USED_FIELD,
        invoice.CREDIT_FIELD,
        invoice.BALANCE_FIELD,
    ]

    DOLLAR_COLUMN_LIST = [
        invoice.RATE_FIELD,
        invoice.GROUP_BALANCE_FIELD,
        invoice.COST_FIELD,
        invoice.GROUP_BALANCE_USED_FIELD,
        invoice.CREDIT_FIELD,
        invoice.BALANCE_FIELD,
    ]

    export_columns_list = [
        invoice.INVOICE_DATE_FIELD,
        invoice.PROJECT_FIELD,
        invoice.PROJECT_ID_FIELD,
        invoice.PI_FIELD,
        invoice.INVOICE_EMAIL_FIELD,
        invoice.INVOICE_ADDRESS_FIELD,
        invoice.INSTITUTION_FIELD,
        invoice.INSTITUTION_ID_FIELD,
        invoice.SU_HOURS_FIELD,
        invoice.SU_TYPE_FIELD,
        invoice.RATE_FIELD,
        invoice.GROUP_NAME_FIELD,
        invoice.GROUP_INSTITUTION_FIELD,
        invoice.GROUP_BALANCE_FIELD,
        invoice.COST_FIELD,
        invoice.GROUP_BALANCE_USED_FIELD,
        invoice.CREDIT_FIELD,
        invoice.CREDIT_CODE_FIELD,
        invoice.BALANCE_FIELD,
    ]

    prepay_credits: pandas.DataFrame

    @staticmethod
    def _get_existing_columns(dataframe: pandas.DataFrame, colum_name_list):
        existing_columns = list()
        for column_name in colum_name_list:
            if column_name in dataframe.columns:
                existing_columns.append(column_name)

        return existing_columns

    def _prepare(self):
        self.export_data = self.data[
            self.data[invoice.IS_BILLABLE_FIELD] & ~self.data[invoice.MISSING_PI_FIELD]
        ]
        self.export_data = self.export_data[
            ~self.export_data[invoice.GROUP_NAME_FIELD].isna()
        ]
        self.group_list = self.export_data[invoice.GROUP_NAME_FIELD].unique()

    def _get_group_dataframe(self, data, group):
        def add_dollar_sign(data):
            if pandas.isna(data):
                return data
            else:
                return "$" + str(data)

        group_projects = (
            data[data[invoice.GROUP_NAME_FIELD] == group].copy().reset_index(drop=True)
        )

        # Add row if group has credit this month
        group_credit_mask = (
            self.prepay_credits[invoice.PREPAY_MONTH_FIELD] == self.invoice_month
        ) & (self.prepay_credits[invoice.PREPAY_GROUP_NAME_FIELD] == group)
        group_credit_info = self.prepay_credits[group_credit_mask]
        if not group_credit_info.empty:
            group_credit = group_credit_info.squeeze()[invoice.PREPAY_CREDIT_FIELD]
            group_projects.loc[len(group_projects)] = None

            existing_columns = self._get_existing_columns(
                group_projects, self.CREDIT_COLUMN_COPY_LIST
            )
            for column_name in existing_columns:
                group_projects.loc[
                    group_projects.index[-1], column_name
                ] = group_projects.loc[0, column_name]

            group_projects.loc[
                group_projects.index[-1], [invoice.COST_FIELD, invoice.BALANCE_FIELD]
            ] = [group_credit] * 2

        # Add sum row
        group_projects.loc[len(group_projects)] = None
        existing_columns = self._get_existing_columns(
            group_projects, self.TOTAL_COLUMN_LIST
        )
        group_projects.loc[
            group_projects.index[-1], invoice.INVOICE_DATE_FIELD
        ] = "Total"
        group_projects.loc[group_projects.index[-1], existing_columns] = group_projects[
            existing_columns
        ].sum()

        # Add dollar signs
        existing_columns = self._get_existing_columns(
            group_projects, self.DOLLAR_COLUMN_LIST
        )
        for column_name in existing_columns:
            group_projects[column_name] = group_projects[column_name].apply(
                add_dollar_sign
            )

        group_projects.fillna("", inplace=True)

        return group_projects

    def export(self):
        def _create_html_invoice(temp_fd):
            environment = Environment(loader=FileSystemLoader(TEMPLATE_DIR_PATH))
            template = environment.get_template("pi_invoice.html")
            content = template.render(
                data=group_dataframe,
            )
            temp_fd.write(content)
            temp_fd.flush()

        def _create_pdf_invoice(temp_fd_name):
            chrome_binary_location = os.environ.get(
                "CHROME_BIN_PATH", "usr/bin/chromium"
            )
            if not os.path.exists(chrome_binary_location):
                sys.exit(
                    f"Chrome binary does not exist at {chrome_binary_location}. Make sure the env var CHROME_BIN_PATH is set correctly or that Google Chrome is installed"
                )

            invoice_pdf_path = f"{self.name}/{group_instituition}_{group_contact_email}_{self.invoice_month}.pdf"
            subprocess.run(
                [
                    chrome_binary_location,
                    "--headless",
                    "--no-sandbox",
                    f"--print-to-pdf={invoice_pdf_path}",
                    "--no-pdf-header-footer",
                    "file://" + temp_fd_name,
                ],
                capture_output=True,
            )

        self._filter_columns()

        if not os.path.exists(self.name):
            os.mkdir(self.name)

        for group in self.group_list:
            group_dataframe = self._get_group_dataframe(self.export_data, group)
            group_instituition = group_dataframe[invoice.GROUP_INSTITUTION_FIELD].iat[0]
            group_contact_email = group_dataframe[invoice.INVOICE_EMAIL_FIELD].iat[0]

            with tempfile.NamedTemporaryFile(mode="w", suffix=".html") as temp_fd:
                _create_html_invoice(temp_fd)
                _create_pdf_invoice(temp_fd.name)

    def export_s3(self, s3_bucket):
        def _export_s3_group_invoice(group_invoice):
            group_invoice_path = os.path.join(self.name, group_invoice)
            striped_invoice_path = os.path.splitext(group_invoice_path)[0]
            output_s3_path = f"Invoices/{self.invoice_month}/{striped_invoice_path}.pdf"
            output_s3_archive_path = f"Invoices/{self.invoice_month}/Archive/{striped_invoice_path} {util.get_iso8601_time()}.pdf"
            s3_bucket.upload_file(group_invoice_path, output_s3_path)
            s3_bucket.upload_file(group_invoice_path, output_s3_archive_path)

        for group_invoice in os.listdir(self.name):
            _export_s3_group_invoice(group_invoice)
