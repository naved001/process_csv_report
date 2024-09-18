import pandas

from process_report.processors import (
    add_institution_processor,
    validate_pi_alias_processor,
    lenovo_processor,
    remove_nonbillables_processor,
    validate_billable_pi_processor,
    new_pi_credit_processor,
)
from process_report.invoices import bu_internal_invoice


def new_bu_internal_invoice(
    name="", invoice_month="0000-00", data=pandas.DataFrame(), subsidy_amount=0
):
    return bu_internal_invoice.BUInternalInvoice(
        name, invoice_month, data, subsidy_amount
    )


def new_add_institution_processor(
    name="",
    invoice_month="0000-00",
    data=pandas.DataFrame(),
):
    return add_institution_processor.AddInstitutionProcessor(name, invoice_month, data)


def new_validate_pi_alias_processor(
    name="", invoice_month="0000-00", data=pandas.DataFrame(), alias_map={}
):
    return validate_pi_alias_processor.ValidatePIAliasProcessor(
        name, invoice_month, data, alias_map
    )


def new_lenovo_processor(name="", invoice_month="0000-00", data=pandas.DataFrame()):
    return lenovo_processor.LenovoProcessor(name, invoice_month, data)


def new_remove_nonbillables_processor(
    name="",
    invoice_month="0000-00",
    data=pandas.DataFrame(),
    nonbillable_pis=[],
    nonbillable_projects=[],
):
    return remove_nonbillables_processor.RemoveNonbillables(
        name, invoice_month, data, nonbillable_pis, nonbillable_projects
    )


def new_validate_billable_pi_processor(
    name="", invoice_month="0000-00", data=pandas.DataFrame()
):
    return validate_billable_pi_processor.ValidateBillablePIsProcessor(
        name, invoice_month, data
    )


def new_new_pi_credit_processor(
    name="",
    invoice_month="0000-00",
    data=pandas.DataFrame(),
    old_pi_filepath="",
):
    return new_pi_credit_processor.NewPICreditProcessor(
        name, invoice_month, data, old_pi_filepath
    )
