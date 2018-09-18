#!/usr/bin/env python

from os.path import exists
from setuptools import setup, find_packages


setup(
    name="streamz_ext",
    version='0.2.1',
    description="Streams",
    url="http://github.com/xpdAcq/streamz_ext/",
    maintainer="Christopher J. Wright",
    maintainer_email="cjwright4242@gmail.com",
    license="BSD",
    keywords="streams",
    packages=find_packages(),
    long_description=(
        open("README.rst").read() if exists("README.rst") else ""
    ),
    # install_requires=list(open("requirements.txt").read().strip().split("\n")),
    zip_safe=False,
)
