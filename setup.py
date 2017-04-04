#!/usr/bin/env python

from setuptools import setup

setup(name='tap-harvest',
      version="0.3.3",
      description='Singer.io tap for extracting data from the Harvest api',
      author='Facet Interactive',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_harvest'],
      install_requires=[
          'singer-python==0.3.1',
          'requests==2.13.0',
          'dateparser==0.6.0',
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
