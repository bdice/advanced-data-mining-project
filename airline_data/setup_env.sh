#!/bin/bash

# Must set Python 2 because Spark 2.2 on flux-hadoop has a bug with Python 3
# that causes extremely slow performance due to executor failures
export PYSPARK_PYTHON=/sw/dsi/centos7/x86-64/Anaconda2-5.0.1/bin/python
export PYSPARK_DRIVER_PYTHON=/sw/dsi/centos7/x86-64/Anaconda2-5.0.1/bin/python
