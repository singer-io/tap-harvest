from tap_harvest.streams.projects import Projects
from tap_harvest.streams.clients import Clients
from tap_harvest.streams.contacts import Contacts
from tap_harvest.streams.estimate_item_categories import EstimateItemCategories
from tap_harvest.streams.estimate_line_items import EstimateLineItems
from tap_harvest.streams.estimate_messages import EstimateMessages
from tap_harvest.streams.estimates import Estimates
from tap_harvest.streams.expense_categories import ExpenseCategories
from tap_harvest.streams.expenses import Expenses
from tap_harvest.streams.external_reference import ExternalReference
from tap_harvest.streams.invoice_item_categories import InvoiceItemCategories
from tap_harvest.streams.invoice_line_items import InvoiceLineItems
from tap_harvest.streams.invoice_messages import InvoiceMessages
from tap_harvest.streams.invoice_payments import InvoicePayments
from tap_harvest.streams.invoices import Invoices
from tap_harvest.streams.project_tasks import ProjectTasks
from tap_harvest.streams.project_users import ProjectUsers
from tap_harvest.streams.roles import Roles
from tap_harvest.streams.tasks import Tasks
from tap_harvest.streams.time_entries import TimeEntries
from tap_harvest.streams.time_entry_external_reference import (
    TimeEntryExternalReference,
)
from tap_harvest.streams.user_project_tasks import UserProjectTasks
from tap_harvest.streams.user_projects import UserProjects
from tap_harvest.streams.user_roles import UserRoles
from tap_harvest.streams.users import Users


STREAMS = {
    "projects": Projects,
    "clients": Clients,
    "contacts": Contacts,
    "estimate_item_categories": EstimateItemCategories,
    "estimate_line_items": EstimateLineItems,
    "estimate_messages": EstimateMessages,
    "estimates": Estimates,
    "expense_categories": ExpenseCategories,
    "expenses": Expenses,
    "external_reference": ExternalReference,
    "invoice_item_categories": InvoiceItemCategories,
    "invoice_line_items": InvoiceLineItems,
    "invoice_messages": InvoiceMessages,
    "invoice_payments": InvoicePayments,
    "invoices": Invoices,
    "project_tasks": ProjectTasks,
    "project_users": ProjectUsers,
    "roles": Roles,
    "tasks": Tasks,
    "time_entries": TimeEntries,
    "time_entry_external_reference": TimeEntryExternalReference,
    "user_project_tasks": UserProjectTasks,
    "user_projects": UserProjects,
    "user_roles": UserRoles,
    "users": Users,
}
