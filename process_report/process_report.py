import argparse
import os
import sys
import datetime
from decimal import Decimal

import json
import pandas
import boto3
import pyarrow


### Invoice field names
INVOICE_DATE_FIELD = "Invoice Month"
PROJECT_FIELD = "Project - Allocation"
PROJECT_ID_FIELD = "Project - Allocation ID"
PI_FIELD = "Manager (PI)"
INVOICE_EMAIL_FIELD = "Invoice Email"
INVOICE_ADDRESS_FIELD = "Invoice Address"
INSTITUTION_FIELD = "Institution"
INSTITUTION_ID_FIELD = "Institution - Specific Code"
SU_HOURS_FIELD = "SU Hours (GBhr or SUhr)"
SU_TYPE_FIELD = "SU Type"
COST_FIELD = "Cost"
CREDIT_FIELD = "Credit"
CREDIT_CODE_FIELD = "Credit Code"
SUBSIDY_FIELD = "Subsidy"
BALANCE_FIELD = "Balance"
###


def get_institution_from_pi(institute_map, pi_uname):
    institution_key = pi_uname.split("@")[-1]
    institution_name = institute_map.get(institution_key, "")

    if institution_name == "":
        print(f"Warning: PI name {pi_uname} does not match any institution!")

    return institution_name


def load_institute_map() -> dict:
    with open("process_report/institute_map.json", "r") as f:
        institute_map = json.load(f)

    return institute_map


def load_old_pis(old_pi_file):
    old_pi_dict = dict()

    try:
        with open(old_pi_file) as f:
            for pi_info in f:
                pi, first_month = pi_info.strip().split(",")
                old_pi_dict[pi] = first_month
    except FileNotFoundError:
        print("Applying credit 0002 failed. Old PI file does not exist")
        sys.exit(1)

    return old_pi_dict


def dump_old_pis(old_pi_file, old_pi_dict: dict):
    with open(old_pi_file, "w") as f:
        for pi, first_month in old_pi_dict.items():
            f.write(f"{pi},{first_month}\n")


def is_old_pi(old_pi_dict, pi, invoice_month):
    first_invoice_month = old_pi_dict.get(pi, invoice_month)
    if compare_invoice_month(first_invoice_month, invoice_month):
        sys.exit(
            f"PI {pi} from {first_invoice_month} found in {invoice_month} invoice!"
        )
    if compare_invoice_month(invoice_month, first_invoice_month):
        return True
    return False


def compare_invoice_month(month_1, month_2):
    """Returns True if 1st date is later than 2nd date"""
    dt1 = datetime.datetime.strptime(month_1, "%Y-%m")
    dt2 = datetime.datetime.strptime(month_2, "%Y-%m")
    return dt1 > dt2


def get_invoice_bucket():
    try:
        s3_resource = boto3.resource(
            service_name="s3",
            endpoint_url=os.environ.get(
                "S3_ENDPOINT", "https://s3.us-east-005.backblazeb2.com"
            ),
            aws_access_key_id=os.environ["S3_KEY_ID"],
            aws_secret_access_key=os.environ["S3_APP_KEY"],
        )
    except KeyError:
        print("Error: Please set the environment variables S3_KEY_ID and S3_APP_KEY")
    return s3_resource.Bucket(os.environ.get("S3_BUCKET_NAME", "nerc-invoicing"))


def get_iso8601_time():
    return datetime.datetime.now().strftime("%Y%m%dT%H%M%SZ")


def main():
    """Remove non-billable PIs and projects"""

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "csv_files",
        nargs="*",
        help="One or more CSV files that need to be processed",
    )
    parser.add_argument(
        "--fetch-from-s3",
        action="store_true",
        help="If set, fetches invoices from S3 storage. Requires environment variables for S3 authentication to be set",
    )
    parser.add_argument(
        "--upload-to-s3",
        action="store_true",
        help="If set, uploads all processed invoices and old PI file to S3",
    )
    parser.add_argument(
        "--invoice-month",
        required=True,
        help="Invoice month to process",
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

    parser.add_argument(
        "--nonbillable-file",
        required=False,
        default="nonbillable.csv",
        help="Name of nonbillable file",
    )
    parser.add_argument(
        "--output-file",
        required=False,
        default="filtered_output.csv",
        help="Name of output file",
    )
    parser.add_argument(
        "--output-folder",
        required=False,
        default="pi_invoices",
        help="Name of output folder containing pi-specific invoice csvs",
    )
    parser.add_argument(
        "--BU-invoice-file",
        required=False,
        default="BU_Internal.csv",
        help="Name of output csv for BU invoices",
    )
    parser.add_argument(
        "--HU-BU-invoice-file",
        required=False,
        default="HU_BU.csv",
        help="Name of output csv for HU and BU invoice",
    )
    parser.add_argument(
        "--Lenovo-file",
        required=False,
        default="Lenovo.csv",
        help="Name of output csv for Lenovo SU Types invoice",
    )
    parser.add_argument(
        "--old-pi-file",
        required=False,
        help="Name of csv file listing previously billed PIs. If not provided, defaults to fetching from S3",
    )
    parser.add_argument(
        "--BU-subsidy-amount",
        required=True,
        type=int,
        help="Amount of subsidy given to BU PIs",
    )
    args = parser.parse_args()

    invoice_month = args.invoice_month

    if args.fetch_from_s3:
        csv_files = fetch_s3_invoices(invoice_month)
    else:
        csv_files = args.csv_files
    if args.old_pi_file:
        old_pi_file = args.old_pi_file
    else:
        old_pi_file = fetch_s3_old_pi_file()

    merged_dataframe = merge_csv(csv_files)

    pi = []
    projects = []
    with open(args.pi_file) as file:
        pi = [line.rstrip() for line in file]
    with open(args.projects_file) as file:
        projects = [line.rstrip() for line in file]

    print("Invoice date: " + str(invoice_month))

    timed_projects_list = timed_projects(args.timed_projects_file, invoice_month)
    print("The following timed-projects will not be billed for this period: ")
    print(timed_projects_list)

    projects = list(set(projects + timed_projects_list))

    merged_dataframe = add_institution(merged_dataframe)
    export_lenovo(merged_dataframe, args.Lenovo_file)
    remove_billables(merged_dataframe, pi, projects, args.nonbillable_file)

    billable_projects = remove_non_billables(merged_dataframe, pi, projects)
    billable_projects = validate_pi_names(billable_projects)
    credited_projects = apply_credits_new_pi(billable_projects, old_pi_file)

    export_billables(credited_projects, args.output_file)
    export_pi_billables(credited_projects, args.output_folder, invoice_month)
    export_BU_only(billable_projects, args.BU_invoice_file, args.BU_subsidy_amount)
    export_HU_BU(credited_projects, args.HU_BU_invoice_file)

    if args.upload_to_s3:
        invoice_list = [
            args.nonbillable_file,
            args.output_file,
            args.Lenovo_file,
        ]

        for pi_invoice in os.listdir(args.output_folder):
            invoice_list.append(os.path.join(args.output_folder, pi_invoice))

        upload_to_s3(invoice_list, invoice_month)
        upload_to_s3_HU_BU(args.HU_BU_invoice_file, invoice_month)
        upload_to_s3_old_pi_file(old_pi_file)


def fetch_s3_invoices(invoice_month):
    """Fetches usage invoices from S3 given invoice month"""
    s3_invoice_list = list()
    invoice_bucket = get_invoice_bucket()
    for obj in invoice_bucket.objects.filter(
        Prefix=f"Invoices/{invoice_month}/Service Invoices/"
    ):
        local_name = obj.key.split("/")[-1]
        s3_invoice_list.append(local_name)
        invoice_bucket.download_file(obj.key, local_name)

    return s3_invoice_list


def merge_csv(files):
    """Merge multiple CSV files and return a single pandas dataframe"""
    dataframes = []
    for file in files:
        dataframe = pandas.read_csv(
            file, dtype={COST_FIELD: pandas.ArrowDtype(pyarrow.decimal128(12, 2))}
        )
        dataframes.append(dataframe)

    merged_dataframe = pandas.concat(dataframes, ignore_index=True)
    merged_dataframe.reset_index(drop=True, inplace=True)
    return merged_dataframe


def get_invoice_date(dataframe):
    """Returns the invoice date as a pandas timestamp object

    Note that it only checks the first entry because it should
    be the same for every row.
    """
    invoice_date_str = dataframe[INVOICE_DATE_FIELD][0]
    invoice_date = pandas.to_datetime(invoice_date_str, format="%Y-%m")
    return invoice_date


def timed_projects(timed_projects_file, invoice_date):
    """Returns list of projects that should be excluded based on dates"""
    dataframe = pandas.read_csv(timed_projects_file)

    # convert to pandas timestamp objects
    dataframe["Start Date"] = pandas.to_datetime(
        dataframe["Start Date"], format="%Y-%m"
    )
    dataframe["End Date"] = pandas.to_datetime(dataframe["End Date"], format="%Y-%m")

    mask = (dataframe["Start Date"] <= invoice_date) & (
        invoice_date <= dataframe["End Date"]
    )
    return dataframe[mask]["Project"].to_list()


def remove_non_billables(dataframe, pi, projects):
    """Removes projects and PIs that should not be billed from the dataframe"""
    filtered_dataframe = dataframe[
        ~dataframe[PI_FIELD].isin(pi) & ~dataframe[PROJECT_FIELD].isin(projects)
    ]
    return filtered_dataframe


def remove_billables(dataframe, pi, projects, output_file):
    """Removes projects and PIs that should be billed from the dataframe

    So this *keeps* the projects/pis that should not be billed.
    """
    filtered_dataframe = dataframe[
        dataframe[PI_FIELD].isin(pi) | dataframe[PROJECT_FIELD].isin(projects)
    ]
    filtered_dataframe.to_csv(output_file, index=False)


def validate_pi_names(dataframe):
    invalid_pi_projects = dataframe[pandas.isna(dataframe[PI_FIELD])]
    for i, row in invalid_pi_projects.iterrows():
        print(f"Warning: Billable project {row[PROJECT_FIELD]} has empty PI field")
    dataframe = dataframe[~pandas.isna(dataframe[PI_FIELD])]

    return dataframe


def apply_credits_new_pi(dataframe, old_pi_file):
    new_pi_credit_code = "0002"
    new_pi_credit_amount = 1000

    dataframe[CREDIT_FIELD] = None
    dataframe[CREDIT_CODE_FIELD] = None
    dataframe[BALANCE_FIELD] = Decimal(0)

    old_pi_dict = load_old_pis(old_pi_file)

    current_pi_list = dataframe[PI_FIELD].unique()
    invoice_month = dataframe[INVOICE_DATE_FIELD].iat[0]

    for pi in current_pi_list:
        pi_projects = dataframe[dataframe[PI_FIELD] == pi]

        if is_old_pi(old_pi_dict, pi, invoice_month):
            for i, row in pi_projects.iterrows():
                dataframe.at[i, BALANCE_FIELD] = row[COST_FIELD]
        else:
            old_pi_dict[pi] = invoice_month
            print(f"Found new PI {pi}")
            remaining_credit = new_pi_credit_amount
            for i, row in pi_projects.iterrows():
                project_cost = row[COST_FIELD]
                applied_credit = min(project_cost, remaining_credit)

                dataframe.at[i, CREDIT_FIELD] = applied_credit
                dataframe.at[i, CREDIT_CODE_FIELD] = new_pi_credit_code
                dataframe.at[i, BALANCE_FIELD] = row[COST_FIELD] - applied_credit
                remaining_credit -= applied_credit

                if remaining_credit == 0:
                    break

    dump_old_pis(old_pi_file, old_pi_dict)

    return dataframe


def fetch_s3_old_pi_file():
    local_name = "PI.csv"
    invoice_bucket = get_invoice_bucket()
    invoice_bucket.download_file("PIs/PI.csv", local_name)
    return local_name


def upload_to_s3_old_pi_file(old_pi_file):
    invoice_bucket = get_invoice_bucket()
    invoice_bucket.upload_file(old_pi_file, "PIs/PI.csv")


def add_institution(dataframe: pandas.DataFrame):
    """Determine every PI's institution name, logging any PI whose institution cannot be determined
    This is performed by `get_institution_from_pi()`, which tries to match the PI's username to
    a list of known institution email domains (i.e bu.edu), or to several edge cases (i.e rudolph) if
    the username is not an email address.

    Exact matches are then mapped to the corresponding institution name.

    I.e "foo@bu.edu" would match with "bu.edu", which maps to the instition name "Boston University"

    The list of mappings are defined in `institute_map.json`.
    """
    institute_map = load_institute_map()
    for i, row in dataframe.iterrows():
        pi_name = row[PI_FIELD]
        if pandas.isna(pi_name):
            print(f"Project {row[PROJECT_FIELD]} has no PI")
        else:
            dataframe.at[i, INSTITUTION_FIELD] = get_institution_from_pi(
                institute_map, pi_name
            )

    return dataframe


def export_billables(dataframe, output_file):
    dataframe.to_csv(output_file, index=False)


def export_pi_billables(dataframe: pandas.DataFrame, output_folder, invoice_month):
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)

    pi_list = dataframe[PI_FIELD].unique()

    for pi in pi_list:
        if pandas.isna(pi):
            continue
        pi_projects = dataframe[dataframe[PI_FIELD] == pi]
        pi_instituition = pi_projects[INSTITUTION_FIELD].iat[0]
        pi_projects.to_csv(
            output_folder + f"/{pi_instituition}_{pi}_{invoice_month}.csv"
        )


def export_BU_only(dataframe: pandas.DataFrame, output_file, subsidy_amount):
    def get_project(row):
        project_alloc = row[PROJECT_FIELD]
        if project_alloc.rfind("-") == -1:
            return project_alloc
        else:
            return project_alloc[: project_alloc.rfind("-")]

    BU_projects = dataframe[dataframe[INSTITUTION_FIELD] == "Boston University"]
    BU_projects["Project"] = BU_projects.apply(get_project, axis=1)
    BU_projects[SUBSIDY_FIELD] = 0
    BU_projects = BU_projects[
        [
            INVOICE_DATE_FIELD,
            PI_FIELD,
            "Project",
            COST_FIELD,
            CREDIT_FIELD,
            SUBSIDY_FIELD,
            BALANCE_FIELD,
        ]
    ]
    BU_projects = _apply_subsidy(BU_projects, subsidy_amount)
    BU_projects.to_csv(output_file)


def _apply_subsidy(dataframe, subsidy_amount):
    pi_list = dataframe[PI_FIELD].unique()

    for pi in pi_list:
        pi_projects = dataframe[dataframe[PI_FIELD] == pi]
        remaining_subsidy = subsidy_amount
        for i, row in pi_projects.iterrows():
            project_remaining_cost = row[BALANCE_FIELD]
            applied_subsidy = min(project_remaining_cost, remaining_subsidy)

            dataframe.at[i, SUBSIDY_FIELD] = applied_subsidy
            dataframe.at[i, BALANCE_FIELD] = row[BALANCE_FIELD] - applied_subsidy
            remaining_subsidy -= applied_subsidy

            if remaining_subsidy == 0:
                break

    return dataframe


def export_HU_BU(dataframe, output_file):
    HU_BU_projects = dataframe[
        (dataframe[INSTITUTION_FIELD] == "Harvard University")
        | (dataframe[INSTITUTION_FIELD] == "Boston University")
    ]
    HU_BU_projects.to_csv(output_file)


def export_lenovo(dataframe: pandas.DataFrame, output_file):
    LENOVO_SU_TYPES = ["OpenShift GPUA100SXM4", "OpenStack GPUA100SXM4"]
    SU_CHARGE_MULTIPLIER = 1

    lenovo_df = dataframe[dataframe[SU_TYPE_FIELD].isin(LENOVO_SU_TYPES)][
        [
            INVOICE_DATE_FIELD,
            PROJECT_FIELD,
            INSTITUTION_FIELD,
            SU_HOURS_FIELD,
            SU_TYPE_FIELD,
        ]
    ]

    lenovo_df.rename(columns={SU_HOURS_FIELD: "SU Hours"}, inplace=True)
    lenovo_df.insert(len(lenovo_df.columns), "SU Charge", SU_CHARGE_MULTIPLIER)
    lenovo_df["Charge"] = lenovo_df["SU Hours"] * lenovo_df["SU Charge"]
    lenovo_df.to_csv(output_file)


def upload_to_s3(invoice_list: list, invoice_month):
    invoice_bucket = get_invoice_bucket()
    for invoice_filename in invoice_list:
        striped_filename = os.path.splitext(invoice_filename)[0]
        invoice_s3_path = (
            f"Invoices/{invoice_month}/{striped_filename} {invoice_month}.csv"
        )
        invoice_s3_path_archive = f"Invoices/{invoice_month}/Archive/{striped_filename} {invoice_month} {get_iso8601_time()}.csv"
        invoice_bucket.upload_file(invoice_filename, invoice_s3_path)
        invoice_bucket.upload_file(invoice_filename, invoice_s3_path_archive)


def upload_to_s3_HU_BU(invoice_filename, invoice_month):
    invoice_bucket = get_invoice_bucket()
    invoice_bucket.upload_file(
        invoice_filename,
        f"Invoices/{invoice_month}/NERC-{invoice_month}-Total-Invoice.csv",
    )
    invoice_bucket.upload_file(
        invoice_filename,
        f"Invoices/{invoice_month}/Archive/NERC-{invoice_month}-Total-Invoice {get_iso8601_time()}.csv",
    )


if __name__ == "__main__":
    main()
