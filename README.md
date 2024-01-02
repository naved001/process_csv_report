# Process CSV reports

## Non-Billable

Automates the process of removing non-billable PIs and projects from the supplied csv report.

A file containing list of PIs may look like:

pi.txt
```
alice@example.com
bob@example.com
foo
bar
```

A file containing list of projects to be excluded may look like:

projects.txt
```
foo
bar
blah blah
```

A file containing list of timed projects will looks like this:
```
PI,Project,Start Date,End Date,Reason
alice@example.com,project foo,2023-09,2024-08,Internal
bo@example.com,project bar,2023-09,2024-08,Internal
```

The script will gather the invoice month from the csv reports and if it falls under the start and end date then those projects will be excluded.
In this example, `project foo` will not be billed for September 2023 and August 2024 and all the months in between for total of 1 year.

The CSV report must have the headers `Manager (PI)'` and `Project - Allocation'`.

```
usage: process_report.py [-h] --pi-file PI_FILE --projects-file PROJECTS_FILE --timed-projects-file TIMED_PROJECTS_FILE [--output-file OUTPUT_FILE]
                         csv_files [csv_files ...]
process_report.py: error: the following arguments are required: csv_files, --pi-file, --projects-file, --timed-projects-file

E.g. python process_report.py test1.csv test2.csv --pi-file pi.txt --projects-file projects.txt --timed-projects-file timed_projects.txt --output-file myfile.csv
```

## Combine CSVs

This script also combines the 3 separate Invoice data CSVs into 1 Invoice CSV. It combines 
OpenShift SU, OpenStack SU, and Storage SU data.
