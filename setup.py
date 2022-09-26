#!/usr/bin/env python

from setuptools import setup

setup(name='tap-harvest',
      version="2.1.2",
      description='Singer.io tap for extracting data from the Harvest api',
      author='Facet Interactive',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_harvest'],
      install_requires=[
          'singer-python==5.12.2',
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
          'tap_harvest': ['tap_harvest/schemas/*.json'],
      },
      include_package_data=True,
)
