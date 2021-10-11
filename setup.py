#!/usr/bin/env python

from setuptools import setup

setup(name='tap-harvest',
      version="2.1.0",
      description='Singer.io tap for extracting data from the Harvest api',
      author='Facet Interactive',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_harvest'],
      install_requires=[
          'singer-python==5.12.1',
          'requests==2.20.0',
          'pendulum==1.2.0',
          'backoff==1.8.0',
          'pytz==2018.4',
      ],
      entry_points='''
          [console_scripts]
          tap-harvest=tap_harvest:main
      ''',
      packages=['tap_harvest'],
      package_data = {
          'tap_harvest/schemas': [
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
