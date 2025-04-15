#!/usr/bin/env python3
import sys
from cassandra.cluster import Cluster


print("this is reducer 2")


# Connect to Cassandra
cluster = Cluster(['cassandra-server'])
session = cluster.connect('search_index')


current_term = None
documents = []
df = 0


def output():
    # Inserting into terms table
    session.execute(
        "INSERT INTO terms (term, document_frequency) VALUES (%s, %s)",
        (current_term, df)
    )
   
    # Inserting into document_index for each document
    for doc_id, tf, positions in documents:
        positions_list = list(map(int, positions.split(',')))
        session.execute(
            """INSERT INTO document_index
               (term, doc_id, term_frequency, positions)
               VALUES (%s, %s, %s, %s)""",
            (current_term, doc_id, tf, positions_list)
        )
       
        # Updating document stats (total terms in document)
        # We'll use the sum of all term frequencies as doc length
        session.execute(
            """UPDATE document_stats
               SET doc_length = doc_length + %s
               WHERE doc_id = %s""",
            (tf, doc_id)
        )


for line in sys.stdin:
    term, doc_id, tf, positions = line.strip().split('\t')
   
    if term != current_term:
        if current_term is not None:
            output()
        current_term = term
        documents = []
        df = 0
   
    documents.append((doc_id, tf, positions))
    df += 1


if current_term is not None:
    output()


cluster.shutdown()