import pandas

from process_report.invoices import (
    invoice,
    billable_invoice,
    pi_specific_invoice,
)

from process_report.processors import (
    add_institution_processor,
    validate_pi_alias_processor,
    lenovo_processor,
    validate_billable_pi_processor,
    new_pi_credit_processor,
    bu_subsidy_processor,
    prepayment_processor,
)


def new_base_invoice(
    name="",
    invoice_month="0000-00",
    data=None,
):
    if data is None:
        data = pandas.DataFrame()
    return invoice.Invoice(name, invoice_month, data)


def new_billable_invoice(
    name="",
    invoice_month="0000-00",
    data=None,
    nonbillable_pis=None,
    nonbillable_projects=None,
    old_pi_filepath="",
    updated_old_pi_df=pandas.DataFrame(),
):
    if data is None:
        data = pandas.DataFrame()
    if nonbillable_pis is None:
        nonbillable_pis = []
    if nonbillable_projects is None:
        nonbillable_projects = []
    return billable_invoice.BillableInvoice(
        name,
        invoice_month,
        data,
        old_pi_filepath,
        updated_old_pi_df,
    )


def new_pi_specific_invoice(
    name="",
    invoice_month="0000-00",
    data=None,
):
    if data is None:
        data = pandas.DataFrame()
    return pi_specific_invoice.PIInvoice(
        name,
        invoice_month,
        data,
    )


def new_add_institution_processor(
    name="",
    invoice_month="0000-00",
    data=None,
):
    if data is None:
        data = pandas.DataFrame()
    return add_institution_processor.AddInstitutionProcessor(name, invoice_month, data)


def new_validate_pi_alias_processor(
    name="", invoice_month="0000-00", data=None, alias_map=None
):
    if data is None:
        data = pandas.DataFrame()
    if alias_map is None:
        alias_map = {}
    return validate_pi_alias_processor.ValidatePIAliasProcessor(
        name, invoice_month, data, alias_map
    )


def new_lenovo_processor(name="", invoice_month="0000-00", data=None):
    if data is None:
        data = pandas.DataFrame()
    return lenovo_processor.LenovoProcessor(name, invoice_month, data)


def new_validate_billable_pi_processor(
    name="",
    invoice_month="0000-00",
    data=None,
    nonbillable_pis=None,
    nonbillable_projects=None,
):
    if data is None:
        data = pandas.DataFrame()
    if nonbillable_pis is None:
        nonbillable_pis = []
    if nonbillable_projects is None:
        nonbillable_projects = []

    return validate_billable_pi_processor.ValidateBillablePIsProcessor(
        name,
        invoice_month,
        data,
        nonbillable_pis,
        nonbillable_projects,
    )


def new_new_pi_credit_processor(
    name="",
    invoice_month="0000-00",
    data=None,
    old_pi_filepath="",
    limit_new_pi_credit_to_partners=False,
):
    if data is None:
        data = pandas.DataFrame()
    return new_pi_credit_processor.NewPICreditProcessor(
        name, invoice_month, data, old_pi_filepath, limit_new_pi_credit_to_partners
    )


def new_bu_subsidy_processor(
    name="",
    invoice_month="0000-00",
    data=None,
    subsidy_amount=0,
):
    if data is None:
        data = pandas.DataFrame()
    return bu_subsidy_processor.BUSubsidyProcessor(
        name, invoice_month, data, subsidy_amount
    )


def new_prepayment_processor(
    name="",
    invoice_month="0000-00",
    data=None,
    prepay_credits=None,
    prepay_debits_filepath="",
    prepay_projects=None,
    prepay_contacts=None,
    upload_to_s3=False,
):
    if prepay_credits is None:
        prepay_credits = pandas.DataFrame()
    if prepay_projects is None:
        prepay_projects = pandas.DataFrame()
    if prepay_contacts is None:
        prepay_contacts = pandas.DataFrame()
    return prepayment_processor.PrepaymentProcessor(
        name,
        invoice_month,
        data,
        prepay_credits,
        prepay_projects,
        prepay_contacts,
        prepay_debits_filepath,
        upload_to_s3,
        False,
    )
