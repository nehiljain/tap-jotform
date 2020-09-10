#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-jotform",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_jotform"],
    install_requires=[
        "singer-python>=5.0.12",
        "tenacity==6.2.0",
        "requests==2.24.0",
        "pendulum==1.2.0"
    ],
    extras_require={
        'dev': [
            'ipdb==0.13.3',
            'pylint==2.6.0',
            'pytest'
        ]
    },
    entry_points="""
    [console_scripts]
    tap-jotform=tap_jotform:main
    """,
    packages=["tap_jotform"],
    package_data = {
        "schemas": ["tap_jotform/schemas/*.json"]
    },
    include_package_data=True,
)
