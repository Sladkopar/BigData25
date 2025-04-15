#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# I had problems with Spark, so I need wheel package
pip install wheel

# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -o .venv.tar.gz

# Collect data
bash prepare_data.sh


# Initialize Cassandra
python3 mapreduce/__init__.py

# Run the indexer
bash index.sh data/sample.txt

# Run the ranker
bash search.sh "this is a query!"