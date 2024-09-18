from dataclasses import dataclass
from decimal import Decimal

from process_report.invoices import invoice
from process_report.processors import discount_processor


@dataclass
class BUSubsidyProcessor(discount_processor.DiscountProcessor):
    IS_DISCOUNT_BY_NERC = False

    subsidy_amount: int

    def _apply_subsidy(self, dataframe, subsidy_amount):
        pi_list = dataframe[invoice.PI_FIELD].unique()

        for pi in pi_list:
            pi_projects = dataframe[dataframe[invoice.PI_FIELD] == pi]
            self.apply_flat_discount(
                dataframe,
                pi_projects,
                subsidy_amount,
                invoice.SUBSIDY_FIELD,
                invoice.BALANCE_FIELD,
            )

        return dataframe

    def _prepare(self):
        def get_project(row):
            project_alloc = row[invoice.PROJECT_FIELD]
            if project_alloc.rfind("-") == -1:
                return project_alloc
            else:
                return project_alloc[: project_alloc.rfind("-")]

        self.data = self.data[
            self.data[invoice.INSTITUTION_FIELD] == "Boston University"
        ].copy()
        self.data["Project"] = self.data.apply(get_project, axis=1)
        self.data[invoice.SUBSIDY_FIELD] = Decimal(0)
        self.data = self.data[
            [
                invoice.INVOICE_DATE_FIELD,
                invoice.PI_FIELD,
                "Project",
                invoice.COST_FIELD,
                invoice.CREDIT_FIELD,
                invoice.SUBSIDY_FIELD,
                invoice.BALANCE_FIELD,
            ]
        ]

    def _process(self):
        self.data = self._apply_bu_subsidy(self.data, self.subsidy_amount)
