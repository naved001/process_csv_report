from dataclasses import dataclass
import logging

import pandas

from process_report.invoices import invoice
from process_report.processors import processor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class ValidateBillablePIsProcessor(processor.Processor):
    nonbillable_pis: list[str]
    nonbillable_projects: list[str]

    @staticmethod
    def _validate_pi_names(data: pandas.DataFrame):
        invalid_pi_projects = data[pandas.isna(data[invoice.PI_FIELD])]
        for i, row in invalid_pi_projects.iterrows():
            if row[invoice.IS_BILLABLE_FIELD]:
                logger.warning(
                    f"Billable project {row[invoice.PROJECT_FIELD]} has empty PI field"
                )
        return pandas.isna(data[invoice.PI_FIELD])

    @staticmethod
    def _get_billables(
        data: pandas.DataFrame,
        nonbillable_pis: list[str],
        nonbillable_projects: list[str],
    ):
        return ~data[invoice.PI_FIELD].isin(nonbillable_pis) & ~data[
            invoice.PROJECT_FIELD
        ].isin(nonbillable_projects)

    def _process(self):
        self.data[invoice.IS_BILLABLE_FIELD] = self._get_billables(
            self.data, self.nonbillable_pis, self.nonbillable_projects
        )
        self.data[invoice.MISSING_PI_FIELD] = self._validate_pi_names(self.data)
