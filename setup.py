#!/usr/bin/env python

from setuptools import setup, find_packages
import os.path

with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'VERSION')) as f:
    version = f.read().strip()

setup(name='tap-harvest',
      version=version,
      description='Taps Harvest data',
      author='Facet Interactive',
      url='https://github.com/facetinteractive/tap-harvest',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_harvest'],
      install_requires=[
            'singer-python>=0.2.1',
            'requests==2.13.0',
            'backoff==1.3.2',
            'python-dateutil==2.6.0',
      ],
      entry_points='''
          [console_scripts]
          tap-harvest=tap_harvest:main
      ''',
      packages=['tap_harvest_schemas'],
      package_data = {
          'tap_harvest_schemas': [
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
          '': [
              'VERSION',
              'LICENSE',
          ]
      }
)
