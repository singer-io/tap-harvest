from tap_harvest.streams.projects import Projects
from tap_harvest.streams.clients import Clients
from tap_harvest.streams.contacts import Contacts
from tap_harvest.streams.estimate_item_categories import Estimate_item_categories
from tap_harvest.streams.estimate_line_items import Estimate_line_items
from tap_harvest.streams.estimate_messages import Estimate_messages
from tap_harvest.streams.estimates import Estimates
from tap_harvest.streams.expense_categories import Expense_categories
from tap_harvest.streams.expenses import Expenses
from tap_harvest.streams.external_reference import External_reference
from tap_harvest.streams.invoice_item_categories import Invoice_item_categories
from tap_harvest.streams.invoice_line_items import Invoice_line_items
from tap_harvest.streams.invoice_messages import Invoice_messages
from tap_harvest.streams.invoice_payments import Invoice_payments
from tap_harvest.streams.invoices import Invoices
from tap_harvest.streams.project_tasks import Project_tasks
from tap_harvest.streams.project_users import Project_users
from tap_harvest.streams.roles import Roles
from tap_harvest.streams.tasks import Tasks
from tap_harvest.streams.time_entries import Time_entries
from tap_harvest.streams.time_entry_external_reference import (
    Time_entry_external_reference,
)
from tap_harvest.streams.user_project_tasks import User_project_tasks
from tap_harvest.streams.user_projects import User_projects
from tap_harvest.streams.user_roles import User_roles
from tap_harvest.streams.users import Users

STREAMS = {
    "projects": Projects,
    "clients": Clients,
    "contacts": Contacts,
    "estimate_item_categories": Estimate_item_categories,
    "estimate_line_items": Estimate_line_items,
    "estimate_messages": Estimate_messages,
    "estimates": Estimates,
    "expense_categories": Expense_categories,
    "expenses": Expenses,
    "external_reference": External_reference,
    "invoice_item_categories": Invoice_item_categories,
    "invoice_line_items": Invoice_line_items,
    "invoice_messages": Invoice_messages,
    "invoice_payments": Invoice_payments,
    "invoices": Invoices,
    "project_tasks": Project_tasks,
    "project_users": Project_users,
    "roles": Roles,
    "tasks": Tasks,
    "time_entries": Time_entries,
    "time_entry_external_reference": Time_entry_external_reference,
    "user_project_tasks": User_project_tasks,
    "user_projects": User_projects,
    "user_roles": User_roles,
    "users": Users,
}
