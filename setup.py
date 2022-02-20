#!/usr/bin/env python3
# -*- coding: utf-8 -*-

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages

version = "0.0.3"

setup(
    name="psql2redis",
    version=version,
    url="https://github.com/uy0ll/psql2redis",
    author="uy0ll",
    author_email="uy0ll@yahoo.com",
    description=("Pure Python Implementation of TheThingStack PSQL Replication Only and copy data"
                 "from local to remote Redis DB."),
    license="Apache 2",
    zip_safe=False,
    include_package_data=True,
    packages=find_packages(include=['psql2redis']),
    install_requires=['psycopg2'],
)
