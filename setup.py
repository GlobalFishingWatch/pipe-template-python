#!/usr/bin/env python

"""
Setup script for pipe-orbcomm
"""

import codecs
import os

from setuptools import find_packages
from setuptools import setup

PACKAGE_NAME='pipe_template'
package = __import__(PACKAGE_NAME)


DEPENDENCIES = [
    "pytest",
    "nose",
    "ujson",
    "pytz",
    "udatetime",
    "newlinejson",
    "pipe-tools==0.1.2",
    "jinja2-cli",
]

# Frozen dependencies for the google cloud dataflow dependency
DATAFLOW_PINNED_DEPENDENCIES = [
    "apache-beam==2.1.0",
    "avro==1.8.2",
    "cachetools==2.0.1",
    "certifi==2017.7.27.1",
    "chardet==3.0.4",
    "crcmod==1.7",
    "dill==0.2.6",
    "enum34==1.1.6",
    "funcsigs==1.0.2",
    "future==0.16.0",
    "futures==3.1.1",
    "gapic-google-cloud-pubsub-v1==0.15.4",
    "google-apitools==0.5.11",
    "google-auth==1.1.0",
    "google-auth-httplib2==0.0.2",
    "google-cloud-bigquery==0.25.0",
    "google-cloud-core==0.25.0",
    "google-cloud-dataflow==2.1.0",
    "google-cloud-pubsub==0.26.0",
    "google-gax==0.15.15",
    "googleapis-common-protos==1.5.2",
    "googledatastore==7.0.1",
    "grpc-google-iam-v1==0.11.3",
    "grpcio==1.6.0",
    "httplib2==0.9.2",
    "idna==2.6",
    "mock==2.0.0",
    "oauth2client==3.0.0",
    "pbr==3.1.1",
    "ply==3.8",
    "proto-google-cloud-datastore-v1==0.90.4",
    "proto-google-cloud-pubsub-v1==0.15.4",
    "protobuf==3.3.0",
    "pyasn1==0.3.5",
    "pyasn1-modules==0.1.4",
    "PyYAML==3.12",
    "requests==2.18.4",
    "rsa==3.4.2",
    "six==1.10.0",
    "urllib3==1.22",
]

with codecs.open('README.md', encoding='utf-8') as f:
    readme = f.read().strip()

with codecs.open('requirements.txt', encoding='utf-8') as f:
    DEPENDENCY_LINKS=[line for line in f]

setup(
    author=package.__author__,
    author_email=package.__email__,
    description=package.__doc__.strip(),
    include_package_data=True,
    install_requires=DEPENDENCIES + DATAFLOW_PINNED_DEPENDENCIES,
    keywords='Template',
    license="Apache 2.0",
    long_description=readme,
    name=PACKAGE_NAME,
    packages=find_packages(exclude=['test*.*', 'tests']),
    url=package.__source__,
    version=package.__version__,
    zip_safe=True,
    dependency_links=DEPENDENCY_LINKS
)

