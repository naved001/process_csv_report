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

The CSV report must have the headers `Manager (PI)'` and `Project - Allocation'`.

```
usage: process_report.py [-h] --report-file REPORT_FILE --pi-file PI_FILE --projects-file PROJECTS_FILE
```

