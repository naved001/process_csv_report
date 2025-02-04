from unittest import TestCase, mock
import tempfile
import pandas
import os

from process_report.tests import util as test_utils


class TestPISpecificInvoice(TestCase):
    def _get_test_invoice(
        self,
        pi,
        institution,
        balance,
        is_billable=None,
        missing_pi=None,
        group_name=None,
    ):
        if not is_billable:
            is_billable = [True for _ in range(len(pi))]

        if not missing_pi:
            missing_pi = [False for _ in range(len(pi))]

        if not group_name:
            group_name = [None for _ in range(len(pi))]

        return pandas.DataFrame(
            {
                "Manager (PI)": pi,
                "Institution": institution,
                "Is Billable": is_billable,
                "Missing PI": missing_pi,
                "Prepaid Group Name": group_name,
                "Prepaid Group Institution": ["" for _ in range(len(pi))],
                "Prepaid Group Balance": [0 for _ in range(len(pi))],
                "Prepaid Group Used": [0 for _ in range(len(pi))],
                "Balance": balance,
            }
        )

    def test_get_pi_dataframe(self):
        def add_dollar_sign(data):
            if pandas.isna(data):
                return data
            else:
                return "$" + str(data)

        test_invoice = self._get_test_invoice(
            ["PI1", "PI1", "PI2", "PI2"],
            [
                "BU",
                "BU",
                "HU",
                "HU",
            ],
            [100, 200, 300, 400],
            group_name=[None, "G1", None, None],
        )
        answer_invoice_pi1 = (
            test_invoice[test_invoice["Manager (PI)"] == "PI1"]
            .copy()
            .reset_index(drop=True)
        )
        answer_invoice_pi1.loc[len(answer_invoice_pi1)] = None
        answer_invoice_pi1.loc[
            answer_invoice_pi1.index[-1], ["Invoice Month", "Balance"]
        ] = ["Total", 300]
        for column_name in [
            "Prepaid Group Balance",
            "Prepaid Group Used",
            "Balance",
        ]:
            answer_invoice_pi1[column_name] = answer_invoice_pi1[column_name].apply(
                add_dollar_sign
            )
        answer_invoice_pi1.fillna("", inplace=True)

        answer_invoice_pi2 = (
            test_invoice[test_invoice["Manager (PI)"] == "PI2"]
            .copy()
            .reset_index(drop=True)
        )
        answer_invoice_pi2.loc[len(answer_invoice_pi2)] = None
        answer_invoice_pi2.loc[
            answer_invoice_pi2.index[-1], ["Invoice Month", "Balance"]
        ] = ["Total", 700]
        answer_invoice_pi2 = answer_invoice_pi2.drop(
            [
                "Prepaid Group Name",
                "Prepaid Group Institution",
                "Prepaid Group Balance",
                "Prepaid Group Used",
            ],
            axis=1,
        )
        answer_invoice_pi2["Balance"] = answer_invoice_pi2["Balance"].apply(
            add_dollar_sign
        )
        answer_invoice_pi2.fillna("", inplace=True)

        pi_inv = test_utils.new_pi_specific_invoice(data=test_invoice)
        output_invoice = pi_inv._get_pi_dataframe(test_invoice, "PI1")
        self.assertTrue(answer_invoice_pi1.equals(output_invoice))

        output_invoice = pi_inv._get_pi_dataframe(test_invoice, "PI2")
        self.assertTrue(answer_invoice_pi2.equals(output_invoice))

    @mock.patch("process_report.invoices.invoice.Invoice._filter_columns")
    def test_export_pi(self, mock_filter_cols):
        invoice_month = "2024-10"
        test_invoice = self._get_test_invoice(
            ["PI1", "PI1", "PI2", "PI2"],
            [
                "BU",
                "BU",
                "HU",
                "HU",
            ],
            [100, 200, 300, 400],
            group_name=[None, "G1", None, None],
        )

        mock_filter_cols.return_value = test_invoice
        output_dir = tempfile.TemporaryDirectory()

        pi_inv = test_utils.new_pi_specific_invoice(
            output_dir.name, invoice_month, data=test_invoice
        )
        pi_inv.process()
        pi_inv.export()
        pi_pdf_1 = f"BU_PI1_{invoice_month}.pdf"
        pi_pdf_2 = f"HU_PI2_{invoice_month}.pdf"
        self.assertIn(pi_pdf_1, os.listdir(output_dir.name))
        self.assertIn(pi_pdf_2, os.listdir(output_dir.name))
        self.assertEqual(
            len(os.listdir(output_dir.name)),
            len(test_invoice["Manager (PI)"].unique()),
        )
