import sys
import logging

from dataclasses import dataclass
import pandas
import pyarrow

from process_report import util
from process_report.invoices import invoice
from process_report.processors import discount_processor


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class NewPICreditProcessor(discount_processor.DiscountProcessor):
    """
    This processor operates on data processed by these Processors:
    - ValidateBillablePIsProcessor
    """

    NEW_PI_CREDIT_CODE = "0002"
    EXCLUDE_SU_TYPES = ["OpenShift GPUA100SXM4", "OpenStack GPUA100SXM4"]
    IS_DISCOUNT_BY_NERC = True

    old_pi_filepath: str
    initial_credit_amount: int
    limit_new_pi_credit_to_partners: bool = False

    @staticmethod
    def _load_old_pis(old_pi_filepath) -> pandas.DataFrame:
        try:
            old_pi_df = pandas.read_csv(
                old_pi_filepath,
                dtype={
                    invoice.PI_INITIAL_CREDITS: pandas.ArrowDtype(
                        pyarrow.decimal128(21, 2)
                    ),
                    invoice.PI_1ST_USED: pandas.ArrowDtype(pyarrow.decimal128(21, 2)),
                    invoice.PI_2ND_USED: pandas.ArrowDtype(pyarrow.decimal128(21, 2)),
                },
            )
        except FileNotFoundError:
            sys.exit("Applying credit 0002 failed. Old PI file does not exist")

        return old_pi_df

    @staticmethod
    def _get_pi_age(old_pi_df: pandas.DataFrame, pi, invoice_month):
        """Returns time difference between current invoice month and PI's first invoice month
        I.e 0 for new PIs
        Will raise an error if the PI'a age is negative, which suggests a faulty invoice, or a program bug"""
        first_invoice_month = old_pi_df.loc[
            old_pi_df[invoice.PI_PI_FIELD] == pi, invoice.PI_FIRST_MONTH
        ]
        if first_invoice_month.empty:
            return 0

        month_diff = util.get_month_diff(invoice_month, first_invoice_month.iat[0])
        if month_diff < 0:
            sys.exit(
                f"PI {pi} from {first_invoice_month} found in {invoice_month} invoice!"
            )
        else:
            return month_diff

    def _filter_partners(self, data):
        active_partnerships = list()
        institute_list = util.load_institute_list()
        for institute_info in institute_list:
            if partnership_start_date := institute_info.get(
                "mghpcc_partnership_start_date"
            ):
                if util.get_month_diff(self.invoice_month, partnership_start_date) >= 0:
                    active_partnerships.append(institute_info["display_name"])

        return data[data[invoice.INSTITUTION_FIELD].isin(active_partnerships)]

    def _filter_excluded_su_types(self, data):
        return data[~(data[invoice.SU_TYPE_FIELD].isin(self.EXCLUDE_SU_TYPES))]

    def _filter_nonbillables(self, data):
        return data[data["Is Billable"]]

    def _filter_missing_pis(self, data):
        return data[~data["Missing PI"]]

    def _get_credit_eligible_projects(self, data: pandas.DataFrame):
        filtered_data = self._filter_nonbillables(data)
        filtered_data = self._filter_missing_pis(filtered_data)
        filtered_data = self._filter_excluded_su_types(filtered_data)
        if self.limit_new_pi_credit_to_partners:
            filtered_data = self._filter_partners(filtered_data)

        return filtered_data

    def _apply_credits_new_pi(
        self, data: pandas.DataFrame, old_pi_df: pandas.DataFrame
    ):
        def get_initial_credit_amount(
            old_pi_df, invoice_month, default_initial_credit_amount
        ):
            first_month_processed_pis = old_pi_df[
                old_pi_df[invoice.PI_FIRST_MONTH] == invoice_month
            ]
            if first_month_processed_pis[
                invoice.PI_INITIAL_CREDITS
            ].empty or pandas.isna(
                new_pi_credit_amount := first_month_processed_pis[
                    invoice.PI_INITIAL_CREDITS
                ].iat[0]
            ):
                new_pi_credit_amount = default_initial_credit_amount

            return new_pi_credit_amount

        new_pi_credit_amount = get_initial_credit_amount(
            old_pi_df, self.invoice_month, self.initial_credit_amount
        )
        logger.info(
            f"New PI Credit set at {new_pi_credit_amount} for {self.invoice_month}"
        )

        credit_eligible_projects = self._get_credit_eligible_projects(data)
        current_pi_set = set(credit_eligible_projects[invoice.PI_FIELD])
        for pi in current_pi_set:
            pi_projects = credit_eligible_projects[
                credit_eligible_projects[invoice.PI_FIELD] == pi
            ]
            pi_age = self._get_pi_age(old_pi_df, pi, self.invoice_month)
            pi_old_pi_entry = old_pi_df.loc[
                old_pi_df[invoice.PI_PI_FIELD] == pi
            ].squeeze()

            if pi_age > 1:
                for i, row in pi_projects.iterrows():
                    data.at[i, invoice.BALANCE_FIELD] = row[invoice.COST_FIELD]
            else:
                if pi_age == 0:
                    if len(pi_old_pi_entry) == 0:
                        pi_entry = [pi, self.invoice_month, new_pi_credit_amount, 0, 0]
                        old_pi_df = pandas.concat(
                            [
                                pandas.DataFrame([pi_entry], columns=old_pi_df.columns),
                                old_pi_df,
                            ],
                            ignore_index=True,
                        )
                        pi_old_pi_entry = old_pi_df.loc[
                            old_pi_df[invoice.PI_PI_FIELD] == pi
                        ].squeeze()

                    remaining_credit = new_pi_credit_amount
                    credit_used_field = invoice.PI_1ST_USED
                elif pi_age == 1:
                    remaining_credit = (
                        pi_old_pi_entry[invoice.PI_INITIAL_CREDITS]
                        - pi_old_pi_entry[invoice.PI_1ST_USED]
                    )
                    credit_used_field = invoice.PI_2ND_USED

                credits_used = self.apply_flat_discount(
                    data,
                    pi_projects,
                    invoice.PI_BALANCE_FIELD,
                    remaining_credit,
                    invoice.CREDIT_FIELD,
                    invoice.BALANCE_FIELD,
                    invoice.CREDIT_CODE_FIELD,
                    self.NEW_PI_CREDIT_CODE,
                )

                if (pi_old_pi_entry[credit_used_field] != 0) and (
                    credits_used != pi_old_pi_entry[credit_used_field]
                ):
                    logger.warning(
                        f"PI file overwritten. PI {pi} previously used ${pi_old_pi_entry[credit_used_field]} of New PI credits, now uses ${credits_used}"
                    )
                old_pi_df.loc[
                    old_pi_df[invoice.PI_PI_FIELD] == pi, credit_used_field
                ] = credits_used

        return (data, old_pi_df)

    def _prepare(self):
        self.data[invoice.CREDIT_FIELD] = None
        self.data[invoice.CREDIT_CODE_FIELD] = None
        self.data[invoice.PI_BALANCE_FIELD] = self.data[invoice.COST_FIELD]
        self.data[invoice.BALANCE_FIELD] = self.data[invoice.COST_FIELD]
        self.old_pi_df = self._load_old_pis(self.old_pi_filepath)

    def _process(self):
        self.data, self.updated_old_pi_df = self._apply_credits_new_pi(
            self.data, self.old_pi_df
        )
