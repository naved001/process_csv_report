from dataclasses import dataclass

import process_report.invoices.invoice as invoice
import process_report.util as util


@dataclass
class NERCTotalInvoice(invoice.Invoice):
    INCLUDED_INSTITUTIONS = [
        "Harvard University",
        "Boston University",
        "University of Rhode Island",
    ]

    @property
    def output_path(self) -> str:
        return f"NERC-{self.invoice_month}-Total-Invoice.csv"

    @property
    def output_s3_key(self) -> str:
        return (
            f"Invoices/{self.invoice_month}/NERC-{self.invoice_month}-Total-Invoice.csv"
        )

    @property
    def output_s3_archive_key(self):
        return f"Invoices/{self.invoice_month}/Archive/NERC-{self.invoice_month}-Total-Invoice {util.get_iso8601_time()}.csv"

    def _prepare_export(self):
        self.data = self.data[
            self.data[invoice.INSTITUTION_FIELD].isin(self.INCLUDED_INSTITUTIONS)
        ].copy()
