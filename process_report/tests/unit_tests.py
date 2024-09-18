from unittest import TestCase, mock
import tempfile
import pandas
import os
import uuid
import math
from textwrap import dedent

from process_report import process_report, util
from process_report.invoices import nonbillable_invoice
from process_report.tests import util as test_utils


class TestGetInvoiceDate(TestCase):
    def test_get_invoice_date(self):
        # The month in sample data is not the same
        data = {"Invoice Month": ["2023-01", "2023-02", "2023-03"]}
        dataframe = pandas.DataFrame(data)

        invoice_date = process_report.get_invoice_date(dataframe)

        self.assertIsInstance(invoice_date, pandas.Timestamp)

        # Assert that the invoice_date is the first item
        expected_date = pandas.Timestamp("2023-01")
        self.assertEqual(invoice_date, expected_date)


class TestTimedProjects(TestCase):
    def setUp(self):
        # Without the dedent method, our data will have leading spaces which
        # messes up the first key. Also the '\' is imporant to ignore the first
        # new line we added so it's more readable in code.
        self.csv_data = dedent(
            """\
        Project,Start Date,End Date
        ProjectA,2022-09,2023-08
        ProjectB,2022-09,2023-09
        ProjectC,2023-09,2024-08
        ProjectD,2022-09,2024-08
        """
        )
        self.invoice_date = pandas.Timestamp("2023-09")

        self.csv_file = tempfile.NamedTemporaryFile(delete=False, mode="w")
        self.csv_file.write(self.csv_data)
        self.csv_file.close()

    def tearDown(self):
        os.remove(self.csv_file.name)

    def test_timed_projects(self):
        excluded_projects = process_report.timed_projects(
            self.csv_file.name, self.invoice_date
        )

        expected_projects = ["ProjectB", "ProjectC", "ProjectD"]
        self.assertEqual(excluded_projects, expected_projects)


class TestRemoveNonBillables(TestCase):
    def setUp(self):
        data = {
            "Manager (PI)": ["PI1", "PI2", "PI3", "PI4", "PI5"],
            "Project - Allocation": [
                "ProjectA",
                "ProjectB",
                "ProjectC",
                "ProjectD",
                "ProjectE",
            ],
            "Untouch Data Column": ["DataA", "DataB", "DataC", "DataD", "DataE"],
        }
        self.dataframe = pandas.DataFrame(data)

        self.pi_to_exclude = ["PI2", "PI3"]
        self.projects_to_exclude = ["ProjectB", "ProjectD"]
        self.nonbillable_invoice = nonbillable_invoice.NonbillableInvoice(
            "Foo", "Foo", self.dataframe, self.pi_to_exclude, self.projects_to_exclude
        )

        self.output_file = tempfile.NamedTemporaryFile(delete=False)
        self.output_file2 = tempfile.NamedTemporaryFile(delete=False)

    def tearDown(self):
        os.remove(self.output_file.name)
        os.remove(self.output_file2.name)

    def test_remove_billables(self):
        self.nonbillable_invoice.process()
        result_df = self.nonbillable_invoice.data

        self.assertIn("PI2", result_df["Manager (PI)"].tolist())
        self.assertIn("PI3", result_df["Manager (PI)"].tolist())
        self.assertIn("PI4", result_df["Manager (PI)"].tolist())
        self.assertIn("ProjectB", result_df["Project - Allocation"].tolist())
        self.assertIn("ProjectC", result_df["Project - Allocation"].tolist())
        self.assertIn("ProjectD", result_df["Project - Allocation"].tolist())

        self.assertNotIn("PI1", result_df["Manager (PI)"].tolist())
        self.assertNotIn("PI5", result_df["Manager (PI)"].tolist())
        self.assertNotIn("ProjectA", result_df["Project - Allocation"].tolist())
        self.assertNotIn("ProjectE", result_df["Project - Allocation"].tolist())


class TestMergeCSV(TestCase):
    def setUp(self):
        self.header = ["ID", "Name", "Age"]
        self.data = [
            [1, "Alice", 25],
            [2, "Bob", 30],
            [3, "Charlie", 28],
        ]

        self.csv_files = []

        for _ in range(3):
            csv_file = tempfile.NamedTemporaryFile(
                delete=False, mode="w", suffix=".csv"
            )
            self.csv_files.append(csv_file)
            dataframe = pandas.DataFrame(self.data, columns=self.header)
            dataframe.to_csv(csv_file, index=False)
            csv_file.close()

    def tearDown(self):
        for csv_file in self.csv_files:
            os.remove(csv_file.name)

    def test_merge_csv(self):
        merged_dataframe = process_report.merge_csv(
            [csv_file.name for csv_file in self.csv_files]
        )

        expected_rows = len(self.data) * 3
        self.assertEqual(
            len(merged_dataframe), expected_rows
        )  # `len` for a pandas dataframe excludes the header row

        # Assert that the headers in the merged DataFrame match the expected headers
        self.assertListEqual(merged_dataframe.columns.tolist(), self.header)


class TestExportPICSV(TestCase):
    def setUp(self):
        data = {
            "Invoice Month": ["2023-01", "2023-01", "2023-01", "2023-01", "2023-01"],
            "Manager (PI)": ["PI1", "PI1", "PI1", "PI2", "PI2"],
            "Institution": ["BU", "BU", "BU", "HU", "HU"],
            "Project - Allocation": [
                "ProjectA",
                "ProjectB",
                "ProjectC",
                "ProjectD",
                "ProjectE",
            ],
            "Untouch Data Column": ["DataA", "DataB", "DataC", "DataD", "DataE"],
        }
        self.dataframe = pandas.DataFrame(data)
        self.invoice_month = data["Invoice Month"][0]

    def test_export_pi(self):
        output_dir = tempfile.TemporaryDirectory()
        process_report.export_pi_billables(
            self.dataframe, output_dir.name, self.invoice_month
        )

        pi_csv_1 = f'{self.dataframe["Institution"][0]}_{self.dataframe["Manager (PI)"][0]}_{self.dataframe["Invoice Month"][0]}.csv'
        pi_csv_2 = f'{self.dataframe["Institution"][3]}_{self.dataframe["Manager (PI)"][3]}_{self.dataframe["Invoice Month"][3]}.csv'
        self.assertIn(pi_csv_1, os.listdir(output_dir.name))
        self.assertIn(pi_csv_2, os.listdir(output_dir.name))
        self.assertEqual(
            len(os.listdir(output_dir.name)),
            len(self.dataframe["Manager (PI)"].unique()),
        )

        pi_df = pandas.read_csv(output_dir.name + "/" + pi_csv_1)
        self.assertEqual(len(pi_df["Manager (PI)"].unique()), 1)
        self.assertEqual(
            pi_df["Manager (PI)"].unique()[0], self.dataframe["Manager (PI)"][0]
        )

        self.assertIn("ProjectA", pi_df["Project - Allocation"].tolist())
        self.assertIn("ProjectB", pi_df["Project - Allocation"].tolist())
        self.assertIn("ProjectC", pi_df["Project - Allocation"].tolist())

        pi_df = pandas.read_csv(output_dir.name + "/" + pi_csv_2)
        self.assertEqual(len(pi_df["Manager (PI)"].unique()), 1)
        self.assertEqual(
            pi_df["Manager (PI)"].unique()[0], self.dataframe["Manager (PI)"][3]
        )

        self.assertIn("ProjectD", pi_df["Project - Allocation"].tolist())
        self.assertIn("ProjectE", pi_df["Project - Allocation"].tolist())
        self.assertNotIn("ProjectA", pi_df["Project - Allocation"].tolist())
        self.assertNotIn("ProjectB", pi_df["Project - Allocation"].tolist())
        self.assertNotIn("ProjectC", pi_df["Project - Allocation"].tolist())


class TestAddInstituteProcessor(TestCase):
    def test_get_pi_institution(self):
        institute_map = {
            "harvard.edu": "Harvard University",
            "bu.edu": "Boston University",
            "bentley.edu": "Bentley",
            "mclean.harvard.edu": "McLean Hospital",
            "northeastern.edu": "Northeastern University",
            "childrens.harvard.edu": "Boston Children's Hospital",
            "meei.harvard.edu": "Massachusetts Eye & Ear",
            "dfci.harvard.edu": "Dana-Farber Cancer Institute",
            "bwh.harvard.edu": "Brigham and Women's Hospital",
            "bidmc.harvard.edu": "Beth Israel Deaconess Medical Center",
        }

        answers = {
            "q@bu.edu": "Boston University",
            "c@mclean.harvard.edu": "McLean Hospital",
            "b@harvard.edu": "Harvard University",
            "e@edu": "",
            "pi@northeastern.edu": "Northeastern University",
            "h@a.b.c.harvard.edu": "Harvard University",
            "c@a.childrens.harvard.edu": "Boston Children's Hospital",
            "d@a-b.meei.harvard.edu": "Massachusetts Eye & Ear",
            "e@dfci.harvard": "",
            "f@bwh.harvard.edu": "Brigham and Women's Hospital",
            "g@bidmc.harvard.edu": "Beth Israel Deaconess Medical Center",
        }

        add_institute_proc = test_utils.new_add_institution_processor()

        for pi_email, answer in answers.items():
            self.assertEqual(
                add_institute_proc._get_institution_from_pi(institute_map, pi_email),
                answer,
            )


class TestValidateAliasProcessor(TestCase):
    def test_validate_alias(self):
        alias_map = {"PI1": ["PI1_1", "PI1_2"], "PI2": ["PI2_1"]}
        test_data = pandas.DataFrame(
            {
                "Manager (PI)": ["PI1", "PI1_1", "PI1_2", "PI2_1", "PI2_1"],
            }
        )
        answer_data = pandas.DataFrame(
            {
                "Manager (PI)": ["PI1", "PI1", "PI1", "PI2", "PI2"],
            }
        )

        validate_pi_alias_proc = test_utils.new_validate_pi_alias_processor(
            data=test_data, alias_map=alias_map
        )
        validate_pi_alias_proc.process()
        self.assertTrue(answer_data.equals(validate_pi_alias_proc.data))


class TestRemoveNonbillablesProcessor(TestCase):
    def test_remove_nonbillables(self):
        pis = [uuid.uuid4().hex for x in range(10)]
        projects = [uuid.uuid4().hex for x in range(10)]
        nonbillable_pis = pis[:3]
        nonbillable_projects = projects[7:]
        billable_pis = pis[3:7]
        data = pandas.DataFrame({"Manager (PI)": pis, "Project - Allocation": projects})

        remove_nonbillables_proc = test_utils.new_remove_nonbillables_processor()
        data = remove_nonbillables_proc._remove_nonbillables(
            data, nonbillable_pis, nonbillable_projects
        )
        self.assertTrue(data[data["Manager (PI)"].isin(nonbillable_pis)].empty)
        self.assertTrue(
            data[data["Project - Allocation"].isin(nonbillable_projects)].empty
        )
        self.assertTrue(data.equals(data[data["Manager (PI)"].isin(billable_pis)]))


class TestValidateBillablePIProcessor(TestCase):
    def test_validate_billables(self):
        test_data = pandas.DataFrame(
            {
                "Manager (PI)": ["PI1", math.nan, "PI1", "PI2", "PI2"],
                "Project - Allocation": [
                    "ProjectA",
                    "ProjectB",
                    "ProjectC",
                    "ProjectD",
                    "ProjectE",
                ],
            }
        )
        self.assertEqual(1, len(test_data[pandas.isna(test_data["Manager (PI)"])]))
        validate_billable_pi_proc = test_utils.new_validate_billable_pi_processor()
        output_data = validate_billable_pi_proc._validate_pi_names(test_data)
        self.assertEqual(0, len(output_data[pandas.isna(output_data["Manager (PI)"])]))


class TestMonthUtils(TestCase):
    def test_get_month_diff(self):
        testcases = [
            (("2024-12", "2024-03"), 9),
            (("2024-12", "2023-03"), 21),
            (("2024-11", "2024-12"), -1),
            (("2024-12", "2025-03"), -3),
        ]
        for arglist, answer in testcases:
            self.assertEqual(util.get_month_diff(*arglist), answer)
        with self.assertRaises(ValueError):
            util.get_month_diff("2024-16", "2025-03")


class TestNewPICreditProcessor(TestCase):
    def _assert_result_invoice_and_old_pi_file(
        self,
        invoice_month,
        test_invoice,
        test_old_pi_filepath,
        answer_invoice,
        answer_old_pi_df,
    ):
        new_pi_credit_proc = test_utils.new_new_pi_credit_processor(
            invoice_month=invoice_month,
            data=test_invoice,
            old_pi_filepath=test_old_pi_filepath,
        )
        new_pi_credit_proc.process()

        answer_invoice = answer_invoice.astype(new_pi_credit_proc.data.dtypes)
        answer_old_pi_df = answer_old_pi_df.astype(
            new_pi_credit_proc.updated_old_pi_df.dtypes
        )

        self.assertTrue(new_pi_credit_proc.data.equals(answer_invoice))
        self.assertTrue(new_pi_credit_proc.updated_old_pi_df.equals(answer_old_pi_df))

    def setUp(self) -> None:
        self.test_old_pi_file = tempfile.NamedTemporaryFile(
            delete=False, mode="w+", suffix=".csv"
        )

    def tearDown(self) -> None:
        os.remove(self.test_old_pi_file.name)

    def test_no_new_pi(self):
        test_invoice = pandas.DataFrame(
            {
                "Manager (PI)": ["PI" for _ in range(3)],
                "Cost": [100 for _ in range(3)],
                "SU Type": ["CPU" for _ in range(3)],
            }
        )

        # Other fields of old PI file not accessed if PI is no longer
        # eligible for new-PI credit
        test_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-01"],
                "Initial Credits": [1000],
            }
        )
        test_old_pi_df.to_csv(self.test_old_pi_file.name, index=False)

        answer_invoice = pandas.concat(
            [
                test_invoice,
                pandas.DataFrame(
                    {
                        "Credit": [None for _ in range(3)],
                        "Credit Code": [None for _ in range(3)],
                        "PI Balance": [100 for _ in range(3)],
                        "Balance": [100 for _ in range(3)],
                    }
                ),
            ],
            axis=1,
        )

        answer_old_pi_df = test_old_pi_df.copy()

        self._assert_result_invoice_and_old_pi_file(
            "2024-06",
            test_invoice,
            self.test_old_pi_file.name,
            answer_invoice,
            answer_old_pi_df,
        )

    def test_one_new_pi(self):
        """Invoice with one completely new PI"""

        # One allocation
        invoice_month = "2024-06"
        test_invoice = pandas.DataFrame(
            {
                "Manager (PI)": ["PI"],
                "Cost": [100],
                "SU Type": ["CPU"],
            }
        )

        test_old_pi_df = pandas.DataFrame(
            columns=[
                "PI",
                "First Invoice Month",
                "Initial Credits",
                "1st Month Used",
                "2nd Month Used",
            ]
        )
        test_old_pi_df.to_csv(self.test_old_pi_file.name, index=False)

        answer_invoice = pandas.concat(
            [
                test_invoice,
                pandas.DataFrame(
                    {
                        "Credit": [100],
                        "Credit Code": ["0002"],
                        "PI Balance": [0],
                        "Balance": [0],
                    }
                ),
            ],
            axis=1,
        )

        answer_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [1000],
                "1st Month Used": [100],
                "2nd Month Used": [0],
            }
        )

        self._assert_result_invoice_and_old_pi_file(
            invoice_month,
            test_invoice,
            self.test_old_pi_file.name,
            answer_invoice,
            answer_old_pi_df,
        )

        # Two allocations, costs partially covered
        test_invoice = pandas.DataFrame(
            {
                "Manager (PI)": ["PI", "PI"],
                "Cost": [500, 1000],
                "SU Type": ["CPU", "CPU"],
            }
        )

        answer_invoice = pandas.concat(
            [
                test_invoice,
                pandas.DataFrame(
                    {
                        "Credit": [500, 500],
                        "Credit Code": ["0002", "0002"],
                        "PI Balance": [0, 500],
                        "Balance": [0, 500],
                    }
                ),
            ],
            axis=1,
        )

        answer_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [1000],
                "1st Month Used": [1000],
                "2nd Month Used": [0],
            }
        )

        self._assert_result_invoice_and_old_pi_file(
            invoice_month,
            test_invoice,
            self.test_old_pi_file.name,
            answer_invoice,
            answer_old_pi_df,
        )

        # Two allocations, costs completely covered
        test_invoice = pandas.DataFrame(
            {
                "Manager (PI)": ["PI", "PI"],
                "Cost": [500, 400],
                "SU Type": ["CPU", "CPU"],
            }
        )

        answer_invoice = pandas.concat(
            [
                test_invoice,
                pandas.DataFrame(
                    {
                        "Credit": [500, 400],
                        "Credit Code": ["0002", "0002"],
                        "PI Balance": [0, 0],
                        "Balance": [0, 0],
                    }
                ),
            ],
            axis=1,
        )

        answer_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [1000],
                "1st Month Used": [900],
                "2nd Month Used": [0],
            }
        )

        self._assert_result_invoice_and_old_pi_file(
            invoice_month,
            test_invoice,
            self.test_old_pi_file.name,
            answer_invoice,
            answer_old_pi_df,
        )

    def test_one_month_pi(self):
        """PI has appeared in invoices for one month"""

        # Remaining credits completely covers costs
        invoice_month = "2024-07"
        test_invoice = pandas.DataFrame(
            {
                "Manager (PI)": ["PI"],
                "Cost": [200],
                "SU Type": ["CPU"],
            }
        )

        test_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [1000],
                "1st Month Used": [500],
                "2nd Month Used": [0],
            }
        )
        test_old_pi_df.to_csv(self.test_old_pi_file.name, index=False)

        answer_invoice = pandas.concat(
            [
                test_invoice,
                pandas.DataFrame(
                    {
                        "Credit": [200],
                        "Credit Code": ["0002"],
                        "PI Balance": [0],
                        "Balance": [0],
                    }
                ),
            ],
            axis=1,
        )

        answer_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [1000],
                "1st Month Used": [500],
                "2nd Month Used": [200],
            }
        )

        self._assert_result_invoice_and_old_pi_file(
            invoice_month,
            test_invoice,
            self.test_old_pi_file.name,
            answer_invoice,
            answer_old_pi_df,
        )

        # Remaining credits partially covers costs
        test_invoice = pandas.DataFrame(
            {
                "Manager (PI)": ["PI"],
                "Cost": [600],
                "SU Type": ["CPU"],
            }
        )

        answer_invoice = pandas.concat(
            [
                test_invoice,
                pandas.DataFrame(
                    {
                        "Credit": [500],
                        "Credit Code": ["0002"],
                        "PI Balance": [100],
                        "Balance": [100],
                    }
                ),
            ],
            axis=1,
        )

        answer_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [1000],
                "1st Month Used": [500],
                "2nd Month Used": [500],
            }
        )

        self._assert_result_invoice_and_old_pi_file(
            invoice_month,
            test_invoice,
            self.test_old_pi_file.name,
            answer_invoice,
            answer_old_pi_df,
        )

    def test_two_new_pi(self):
        """Two PIs of different age"""

        # Costs partially and completely covered
        invoice_month = "2024-07"
        test_invoice = pandas.DataFrame(
            {
                "Manager (PI)": ["PI1", "PI1", "PI2"],
                "Cost": [800, 500, 500],
                "SU Type": ["CPU", "CPU", "CPU"],
            }
        )

        test_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI1"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [1000],
                "1st Month Used": [500],
                "2nd Month Used": [0],
            }
        )
        test_old_pi_df.to_csv(self.test_old_pi_file.name, index=False)

        answer_invoice = pandas.concat(
            [
                test_invoice,
                pandas.DataFrame(
                    {
                        "Credit": [500, None, 500],
                        "Credit Code": ["0002", None, "0002"],
                        "PI Balance": [300, 500, 0],
                        "Balance": [300, 500, 0],
                    }
                ),
            ],
            axis=1,
        )

        answer_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI1", "PI2"],
                "First Invoice Month": ["2024-06", "2024-07"],
                "Initial Credits": [1000, 1000],
                "1st Month Used": [500, 500],
                "2nd Month Used": [500, 0],
            }
        )

        self._assert_result_invoice_and_old_pi_file(
            invoice_month,
            test_invoice,
            self.test_old_pi_file.name,
            answer_invoice,
            answer_old_pi_df,
        )

    def test_old_pi_file_overwritten(self):
        """If PI already has entry in Old PI file,
        their initial credits and PI entry could be overwritten"""

        invoice_month = "2024-06"
        test_invoice = pandas.DataFrame(
            {
                "Manager (PI)": ["PI", "PI"],
                "Cost": [500, 500],
                "SU Type": ["CPU", "CPU"],
            }
        )

        test_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [500],
                "1st Month Used": [200],
                "2nd Month Used": [0],
            }
        )
        test_old_pi_df.to_csv(self.test_old_pi_file.name, index=False)

        answer_invoice = pandas.concat(
            [
                test_invoice,
                pandas.DataFrame(
                    {
                        "Credit": [500, None],
                        "Credit Code": ["0002", None],
                        "PI Balance": [0, 500],
                        "Balance": [0, 500],
                    }
                ),
            ],
            axis=1,
        )

        answer_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [500],
                "1st Month Used": [500],
                "2nd Month Used": [0],
            }
        )

        self._assert_result_invoice_and_old_pi_file(
            invoice_month,
            test_invoice,
            self.test_old_pi_file.name,
            answer_invoice,
            answer_old_pi_df,
        )

    def test_excluded_su_types(self):
        """Certain SU types can be excluded from the credit"""

        invoice_month = "2024-06"
        test_invoice = pandas.DataFrame(
            {
                "Manager (PI)": ["PI", "PI", "PI", "PI"],
                "Cost": [600, 600, 600, 600],
                "SU Type": [
                    "CPU",
                    "OpenShift GPUA100SXM4",
                    "GPU",
                    "OpenStack GPUA100SXM4",
                ],
            }
        )

        test_old_pi_df = pandas.DataFrame(
            columns=[
                "PI",
                "First Invoice Month",
                "Initial Credits",
                "1st Month Used",
                "2nd Month Used",
            ]
        )
        test_old_pi_df.to_csv(self.test_old_pi_file.name, index=False)

        answer_invoice = pandas.concat(
            [
                test_invoice,
                pandas.DataFrame(
                    {
                        "Credit": [600, None, 400, None],
                        "Credit Code": ["0002", None, "0002", None],
                        "PI Balance": [0, 600, 200, 600],
                        "Balance": [0, 600, 200, 600],
                    }
                ),
            ],
            axis=1,
        )

        answer_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [1000],
                "1st Month Used": [1000],
                "2nd Month Used": [0],
            }
        )

        self._assert_result_invoice_and_old_pi_file(
            invoice_month,
            test_invoice,
            self.test_old_pi_file.name,
            answer_invoice,
            answer_old_pi_df,
        )

    def test_apply_credit_error(self):
        """Test faulty data"""
        old_pi_df = pandas.DataFrame(
            {"PI": ["PI1"], "First Invoice Month": ["2024-04"]}
        )
        invoice_month = "2024-03"
        test_invoice = test_utils.new_new_pi_credit_processor()
        with self.assertRaises(SystemExit):
            test_invoice._get_pi_age(old_pi_df, "PI1", invoice_month)


class TestBUSubsidyProcessor(TestCase):
    def test_apply_subsidy(self):
        subsidy_amount = 500
        invoice_month = "2024-06"
        test_invoice = pandas.DataFrame(
            {
                "Manager (PI)": ["PI1", "PI1", "PI2", "PI2", "PI3", "PI3"],
                "Institution": [
                    "Boston University",
                    "Boston University",
                    "Harvard University",  # Test case for non-BU PIs
                    "Harvard University",
                    "Boston University",
                    "Boston University",
                ],
                "Project - Allocation": [
                    "P1-A1",
                    "P2-A1",
                    "P3-A1",
                    "P3-A2",
                    "P4",  # 2 Test cases for correctly extracting project name
                    "P4-P4-A1",
                ],
                "PI Balance": [400, 600, 1000, 2000, 500, 500],
                "Balance": [400, 600, 1000, 2000, 500, 500],
            }
        )

        answer_invoice = test_invoice.copy()
        answer_invoice["Project"] = ["P1", "P2", "P3", "P3", "P4", "P4-P4"]
        answer_invoice["BU Balance"] = [400, 100, 0, 0, 500, 0]
        answer_invoice["PI Balance"] = [0, 500, 1000, 2000, 0, 500]

        bu_subsidy_proc = test_utils.new_bu_subsidy_processor(
            invoice_month=invoice_month,
            data=test_invoice,
            subsidy_amount=subsidy_amount,
        )
        bu_subsidy_proc.process()

        answer_invoice = answer_invoice.astype(bu_subsidy_proc.data.dtypes)

        self.assertTrue(bu_subsidy_proc.data.equals(answer_invoice))


class TestLenovoProcessor(TestCase):
    def test_process_lenovo(self):
        test_invoice = pandas.DataFrame(
            {
                "SU Hours (GBhr or SUhr)": [1, 10, 100, 4, 432, 10],
            }
        )
        answer_invoice = test_invoice.copy()
        answer_invoice["SU Charge"] = 1
        answer_invoice["Charge"] = (
            answer_invoice["SU Hours (GBhr or SUhr)"] * answer_invoice["SU Charge"]
        )

        lenovo_proc = test_utils.new_lenovo_processor(data=test_invoice)
        lenovo_proc.process()
        self.assertTrue(lenovo_proc.data.equals(answer_invoice))


class TestUploadToS3(TestCase):
    @mock.patch("process_report.process_report.get_invoice_bucket")
    @mock.patch("process_report.process_report.get_iso8601_time")
    def test_remove_prefix(self, mock_get_time, mock_get_bucket):
        mock_bucket = mock.MagicMock()
        mock_get_bucket.return_value = mock_bucket
        mock_get_time.return_value = "0"

        invoice_month = "2024-03"
        filenames = ["test.csv", "test2.test.csv", "test3"]
        answers = [
            ("test.csv", f"Invoices/{invoice_month}/test {invoice_month}.csv"),
            (
                "test.csv",
                f"Invoices/{invoice_month}/Archive/test {invoice_month} 0.csv",
            ),
            (
                "test2.test.csv",
                f"Invoices/{invoice_month}/test2.test {invoice_month}.csv",
            ),
            (
                "test2.test.csv",
                f"Invoices/{invoice_month}/Archive/test2.test {invoice_month} 0.csv",
            ),
            ("test3", f"Invoices/{invoice_month}/test3 {invoice_month}.csv"),
            ("test3", f"Invoices/{invoice_month}/Archive/test3 {invoice_month} 0.csv"),
        ]

        process_report.upload_to_s3(filenames, invoice_month)
        for i, call_args in enumerate(mock_bucket.upload_file.call_args_list):
            self.assertTrue(answers[i] in call_args)


class TestBaseInvoice(TestCase):
    def test_filter_exported_columns(self):
        test_invoice = pandas.DataFrame(columns=["C1", "C2", "C3", "C4", "C5"])
        answer_invoice = pandas.DataFrame(columns=["C1", "C3R", "C5R"])
        inv = test_utils.new_base_invoice(data=test_invoice)
        inv.export_columns_list = ["C1", "C3", "C5"]
        inv.exported_columns_map = {"C3": "C3R", "C5": "C5R"}
        inv._filter_columns()

        self.assertTrue(inv.data.equals(answer_invoice))
