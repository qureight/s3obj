#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import find_packages, setup

__version__ = "0.1.0"

with open("README.md") as readme_file:
    readme = readme_file.read()

with open("requirements.txt") as requirements_file:
    requirements = requirements_file.read().splitlines()



setup(
    name="s3obj",
    version=__version__,
    author="F. K.",
    license="Apache License 2.0",
    packages=find_packages(include=["s3obj", "s3obj.*"]),
    long_description=readme,
    classifiers=[
        "Programming Language :: Python :: 3.7",
    ],
    install_requires=requirements
)
