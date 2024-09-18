from dataclasses import dataclass

from process_report.invoices import invoice


@dataclass
class BUInternalInvoice(invoice.Invoice):
    subsidy_amount: int

    def _prepare_export(self):
        self.data = self.data[
            [
                invoice.INVOICE_DATE_FIELD,
                invoice.PI_FIELD,
                "Project",
                invoice.COST_FIELD,
                invoice.CREDIT_FIELD,
                invoice.BU_BALANCE_FIELD,
                invoice.BALANCE_FIELD,
            ]
        ]

        self.data = self._sum_project_allocations(self.data)

    def _sum_project_allocations(self, dataframe):
        """A project may have multiple allocations, and therefore multiple rows
        in the raw invoices. For BU-Internal invoice, we only want 1 row for
        each unique project, summing up its allocations' costs"""
        project_list = dataframe["Project"].unique()
        data_no_dup = dataframe.drop_duplicates("Project", inplace=False)
        sum_fields = [invoice.COST_FIELD, invoice.CREDIT_FIELD, invoice.BALANCE_FIELD]
        for project in project_list:
            project_mask = dataframe["Project"] == project
            no_dup_project_mask = data_no_dup["Project"] == project

            sum_fields_sums = dataframe[project_mask][sum_fields].sum().values
            data_no_dup.loc[no_dup_project_mask, sum_fields] = sum_fields_sums

        return data_no_dup
