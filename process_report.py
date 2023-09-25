import argparse

import csv
import pandas

from datetime import datetime

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

    timed_projects_list = timed_projects(args.timed_projects_file)

    projects = list(set(projects + timed_projects_list))

    remove_non_billables(args.report_file, pi, projects)


def timed_projects(timed_projects_file):
    """Returns list of projects that should be excluded based on dates"""
    timed_projects_list = []
    dataframe = pandas.read_csv(timed_projects_file)

    # convert to datetime objects
    dataframe['Start Date'] = pandas.to_datetime(dataframe['Start Date'], format="%Y-%m")
    dataframe['End Date'] = pandas.to_datetime(dataframe['End Date'], format="%Y-%m")

    current_date = datetime.now()
    for index, row in dataframe.iterrows():
        if row['Start Date'] <= current_date <= row['End Date']:
            timed_projects_list.append(row['Project'])

    return timed_projects_list


def remove_non_billables(report_file, pi, projects):
    dataframe = pandas.read_csv(report_file)
    filtered_dataframe = dataframe[~dataframe['Manager (PI)'].isin(pi) & ~dataframe['Project - Allocation'].isin(projects)]
    filtered_dataframe.to_csv('filtered_' + report_file, index=False)

if __name__ == "__main__":
    main()
