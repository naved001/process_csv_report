import argparse

import csv
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
    args = parser.parse_args()
    remove_non_billables(args.report_file, args.pi_file, args.projects_file)


def remove_non_billables(report_file, pi_file, projects_file):
    pi = []
    projects = []

    with open(pi_file) as file:
        pi = [line.rstrip() for line in file]

    with open(projects_file) as file:
        projects = [line.rstrip() for line in file]

    dataframe = pandas.read_csv(report_file)
    filtered_dataframe = dataframe[~dataframe['Manager (PI)'].isin(pi) & ~dataframe['Project - Allocation'].isin(projects)]
    filtered_dataframe.to_csv(report_file + '_filtered', index=False)

if __name__ == "__main__":
    main()
