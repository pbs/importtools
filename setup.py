#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='importtools',
    version='0.1.5',
    description='Import tools for various data sets.',
    author='Sever Banesiu',
    author_email='banesiu.sever@gmail.com',
    url='https://github.com/pbs/importtools',
    packages=find_packages(),
    setup_requires=['nose>=1.0', 'coverage'],
)
