import argparse

import pandas


def main():
    """Remove non-billable PIs and projects"""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--report-file",
        required=True,
        help="The CSV file with everything in it",
    )
    parser.add_argument(
        "--pi-file",
        required=True,
        help="File containing list of PIs that are non-billable",
    )
    parser.add_argument(
        "--projects-file",
        required=True,
        help="File containing list of projects that are non-billable",
    )
    parser.add_argument(
        "--timed-projects-file",
        required=True,
        help="File containing list of projects that are non-billable within a specified duration",
    )
    args = parser.parse_args()

    pi = []
    projects = []
    with open(args.pi_file) as file:
        pi = [line.rstrip() for line in file]
    with open(args.projects_file) as file:
        projects = [line.rstrip() for line in file]

    invoice_date = get_invoice_date(args.report_file)
    print("Invoice date: " + str(invoice_date))

    timed_projects_list = timed_projects(args.timed_projects_file, invoice_date)
    print("The following timed-projects will not be billed for this period: ")
    print(timed_projects_list)

    projects = list(set(projects + timed_projects_list))

    remove_non_billables(args.report_file, pi, projects)


def get_invoice_date(report_file):
    """Returns the invoice date as a pandas timestamp object

    Note that it only checks the first entry because it should
    be the same for every row.
    """
    dataframe = pandas.read_csv(report_file)
    invoice_date_str = dataframe['Invoice Month'][0]
    invoice_date = pandas.to_datetime(invoice_date_str, format='%Y-%m')
    return invoice_date


def timed_projects(timed_projects_file, invoice_date):
    """Returns list of projects that should be excluded based on dates"""
    dataframe = pandas.read_csv(timed_projects_file)

    # convert to pandas timestamp objects
    dataframe['Start Date'] = pandas.to_datetime(dataframe['Start Date'], format="%Y-%m")
    dataframe['End Date'] = pandas.to_datetime(dataframe['End Date'], format="%Y-%m")

    mask = (dataframe['Start Date'] <= invoice_date) & (invoice_date <= dataframe['End Date'])
    return dataframe[mask]['Project'].to_list()


def remove_non_billables(report_file, pi, projects):
    """Removes projects and PIs that should not be billed from the CSV report_file"""
    dataframe = pandas.read_csv(report_file)
    filtered_dataframe = dataframe[~dataframe['Manager (PI)'].isin(pi) & ~dataframe['Project - Allocation'].isin(projects)]
    filtered_dataframe.to_csv('filtered_' + report_file, index=False)

if __name__ == "__main__":
    main()
