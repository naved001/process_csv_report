# Process CSV reports

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
alice@example.com,project foo,2023-09,2024-09,Internal
bo@example.com,project bar,2023-09,2024-09,Internal
```

Note that a date `2023-09` will be read as `2023-09-01 00:00:00`. So, if a project end date is specified as `2023-09` but the script is executed on `2023-09-02`, the project will no longer be excluded (assuming we met the start date condition).

The CSV report must have the headers `Manager (PI)'` and `Project - Allocation'`.

```
usage: process_report.py [-h] --report-file REPORT_FILE --pi-file PI_FILE --projects-file PROJECTS_FILE --timed-projects-file TIMED_PROJECTS_FILE```

