#!/usr/bin/env python

from setuptools import setup

setup(
    name='sparksql',
    version='1.0.0',
    description='BDCC Pyspark SQL project',
    py_moudles=['__main__'],
    packages=['src.main.python'],
    zip_safe=False
)
