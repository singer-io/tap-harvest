#!/usr/bin/env python

from setuptools import setup, find_packages


setup(
    name="tap-harvest",
    version="3.1.2",
    description="Singer.io tap for extracting data from the Harvest api",
    author="Facet Interactive",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_harvest"],
    install_requires=[
        "singer-python==6.8.0",
        "requests==2.34.2",
        "backoff==2.2.1",
    ],
    entry_points="""
          [console_scripts]
          tap-harvest=tap_harvest:main
      """,
    packages=find_packages(),
    package_data={
        "tap_harvest/schemas": [
            "clients.json",
            "contacts.json",
            "estimate_item_categories.json",
            "estimate_line_items.json",
            "estimate_messages.json",
            "estimates.json",
            "expense_categories.json",
            "expenses.json",
            "external_reference.json",
            "invoice_item_categories.json",
            "invoice_messages.json",
            "invoice_payments.json",
            "invoices.json",
            "project_tasks.json",
            "project_users.json",
            "projects.json",
            "roles.json",
            "tasks.json",
            "time_entries.json",
            "time_entry_external_reference.json",
            "user_project_tasks.json",
            "user_projects.json",
            "user_roles.json",
            "users.json",
        ],
    },
    include_package_data=True,
)
