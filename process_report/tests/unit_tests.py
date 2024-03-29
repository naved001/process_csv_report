from unittest import TestCase
import tempfile
import pandas
import os
from textwrap import dedent
from process_report import process_report

class TestGetInvoiceDate(TestCase):
    def test_get_invoice_date(self):
        # The month in sample data is not the same
        data = {'Invoice Month': ['2023-01', '2023-02', '2023-03']}
        dataframe = pandas.DataFrame(data)

        invoice_date = process_report.get_invoice_date(dataframe)

        self.assertIsInstance(invoice_date, pandas.Timestamp)

        # Assert that the invoice_date is the first item
        expected_date = pandas.Timestamp('2023-01')
        self.assertEqual(invoice_date, expected_date)


class TestTimedProjects(TestCase):
    def setUp(self):

        # Without the dedent method, our data will have leading spaces which
        # messes up the first key. Also the '\' is imporant to ignore the first
        # new line we added so it's more readable in code.
        self.csv_data = dedent("""\
        Project,Start Date,End Date
        ProjectA,2022-09,2023-08
        ProjectB,2022-09,2023-09
        ProjectC,2023-09,2024-08
        ProjectD,2022-09,2024-08
        """)
        self.invoice_date = pandas.Timestamp('2023-09')

        self.csv_file = tempfile.NamedTemporaryFile(delete=False, mode='w')
        self.csv_file.write(self.csv_data)
        self.csv_file.close()

    def tearDown(self):
        os.remove(self.csv_file.name)

    def test_timed_projects(self):
        excluded_projects = process_report.timed_projects(self.csv_file.name, self.invoice_date)

        expected_projects = ['ProjectB', 'ProjectC', 'ProjectD']
        self.assertEqual(excluded_projects, expected_projects)


class TestRemoveNonBillables(TestCase):
    def setUp(self):

        data = {
            'Manager (PI)': ['PI1', 'PI2', 'PI3', 'PI4', 'PI5'],
            'Project - Allocation': ['ProjectA', 'ProjectB', 'ProjectC', 'ProjectD', 'ProjectE'],
            'Untouch Data Column': ['DataA', 'DataB', 'DataC', 'DataD', 'DataE']
        }
        self.dataframe = pandas.DataFrame(data)

        self.pi_to_exclude = ['PI2', 'PI3']
        self.projects_to_exclude = ['ProjectB', 'ProjectD']

        self.output_file = tempfile.NamedTemporaryFile(delete=False)
        self.output_file2 = tempfile.NamedTemporaryFile(delete=False)

    def tearDown(self):
        os.remove(self.output_file.name)
        os.remove(self.output_file2.name)

    def test_remove_non_billables(self):
        process_report.remove_non_billables(self.dataframe, self.pi_to_exclude, self.projects_to_exclude, self.output_file.name)

        result_df = pandas.read_csv(self.output_file.name)

        self.assertNotIn('PI2', result_df['Manager (PI)'].tolist())
        self.assertNotIn('PI3', result_df['Manager (PI)'].tolist())
        self.assertNotIn('PI4', result_df['Manager (PI)'].tolist()) # indirect because ProjectD was removed
        self.assertNotIn('ProjectB', result_df['Project - Allocation'].tolist())
        self.assertNotIn('ProjectC', result_df['Project - Allocation'].tolist()) # indirect because PI3 was removed
        self.assertNotIn('ProjectD', result_df['Project - Allocation'].tolist())

        self.assertIn('PI1', result_df['Manager (PI)'].tolist())
        self.assertIn('PI5', result_df['Manager (PI)'].tolist())
        self.assertIn('ProjectA', result_df['Project - Allocation'].tolist())
        self.assertIn('ProjectE', result_df['Project - Allocation'].tolist())

    def test_remove_billables(self):
        process_report.remove_billables(self.dataframe, self.pi_to_exclude, self.projects_to_exclude, self.output_file2.name)

        result_df = pandas.read_csv(self.output_file2.name)

        self.assertIn('PI2', result_df['Manager (PI)'].tolist())
        self.assertIn('PI3', result_df['Manager (PI)'].tolist())
        self.assertIn('PI4', result_df['Manager (PI)'].tolist())
        self.assertIn('ProjectB', result_df['Project - Allocation'].tolist())
        self.assertIn('ProjectC', result_df['Project - Allocation'].tolist())
        self.assertIn('ProjectD', result_df['Project - Allocation'].tolist())

        self.assertNotIn('PI1', result_df['Manager (PI)'].tolist())
        self.assertNotIn('PI5', result_df['Manager (PI)'].tolist())
        self.assertNotIn('ProjectA', result_df['Project - Allocation'].tolist())
        self.assertNotIn('ProjectE', result_df['Project - Allocation'].tolist())


class TestMergeCSV(TestCase):
    def setUp(self):
        self.header = ['ID', 'Name', 'Age']
        self.data = [
            [1, 'Alice', 25],
            [2, 'Bob', 30],
            [3, 'Charlie', 28],
        ]

        self.csv_files = []

        for _ in range(3):
            csv_file = tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.csv')
            self.csv_files.append(csv_file)
            dataframe = pandas.DataFrame(self.data, columns=self.header)
            dataframe.to_csv(csv_file, index=False)
            csv_file.close()

    def tearDown(self):
        for csv_file in self.csv_files:
            os.remove(csv_file.name)

    def test_merge_csv(self):
        merged_dataframe = process_report.merge_csv([csv_file.name for csv_file in self.csv_files])

        expected_rows = len(self.data) * 3
        self.assertEqual(len(merged_dataframe), expected_rows) # `len` for a pandas dataframe excludes the header row

        # Assert that the headers in the merged DataFrame match the expected headers
        self.assertListEqual(merged_dataframe.columns.tolist(), self.header)


class TestExportPICSV(TestCase):
    def setUp(self):

        data = {
            'Invoice Month': ['2023-01','2023-01','2023-01','2023-01','2023-01'],
            'Manager (PI)': ['PI1', 'PI1', 'PI1', 'PI2', 'PI2'],
            'Institution': ['BU', 'BU', 'BU', 'HU', 'HU'],
            'Project - Allocation': ['ProjectA', 'ProjectB', 'ProjectC', 'ProjectD', 'ProjectE'],
            'Untouch Data Column': ['DataA', 'DataB', 'DataC', 'DataD', 'DataE']
        }
        self.dataframe = pandas.DataFrame(data)

    def test_export_pi(self):
        output_dir = tempfile.TemporaryDirectory()
        process_report.export_pi_billables(self.dataframe, output_dir.name)

        pi_csv_1 = f'{self.dataframe["Institution"][0]}_{self.dataframe["Manager (PI)"][0]}_{self.dataframe["Invoice Month"][0]}.csv'
        pi_csv_2 = f'{self.dataframe["Institution"][3]}_{self.dataframe["Manager (PI)"][3]}_{self.dataframe["Invoice Month"][3]}.csv'
        self.assertIn(pi_csv_1, os.listdir(output_dir.name))
        self.assertIn(pi_csv_2, os.listdir(output_dir.name))
        self.assertEqual(len(os.listdir(output_dir.name)), len(self.dataframe['Manager (PI)'].unique()))

        pi_df = pandas.read_csv(output_dir.name + '/' + pi_csv_1)
        self.assertEqual(len(pi_df['Manager (PI)'].unique()), 1)
        self.assertEqual(pi_df['Manager (PI)'].unique()[0], self.dataframe['Manager (PI)'][0])

        self.assertIn('ProjectA', pi_df['Project - Allocation'].tolist())
        self.assertIn('ProjectB', pi_df['Project - Allocation'].tolist())
        self.assertIn('ProjectC', pi_df['Project - Allocation'].tolist())

        pi_df = pandas.read_csv(output_dir.name + '/' + pi_csv_2)
        self.assertEqual(len(pi_df['Manager (PI)'].unique()), 1)
        self.assertEqual(pi_df['Manager (PI)'].unique()[0], self.dataframe['Manager (PI)'][3])

        self.assertIn('ProjectD', pi_df['Project - Allocation'].tolist())
        self.assertIn('ProjectE', pi_df['Project - Allocation'].tolist())
        self.assertNotIn('ProjectA', pi_df['Project - Allocation'].tolist())
        self.assertNotIn('ProjectB', pi_df['Project - Allocation'].tolist())
        self.assertNotIn('ProjectC', pi_df['Project - Allocation'].tolist())
