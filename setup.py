#!/usr/bin/env python

from setuptools import setup

setup(name='tap-harvest',
      version="0.4.3",
      description='Singer.io tap for extracting data from the Harvest api',
      author='Facet Interactive',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_harvest'],
      install_requires=[
          'singer-python==1.2.0',
          'requests==2.13.0',
          'pendulum==1.2.0',
          'backoff==1.3.2'
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
              "expense_categories.json",
              "expenses.json",
              "invoice_item_categories.json",
              "invoice_payments.json"
              "invoices.json",
              "people.json",
              "project_tasks.json",
              "project_users.json",
              "projects.json",
              "tasks.json",
              "time_entries.json",
          ],
      },
      include_package_data=True,
)
