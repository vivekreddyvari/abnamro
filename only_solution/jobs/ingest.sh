#!/bin/bash

# Set the JAVA_HOME environment variable
export JAVA_HOME=/usr/libexec/java_home

# Start the PySpark shell
pyspark

# Run the PySpark script
spark-submit ../abnamro/data_transformations/ingest.py