#!/usr/bin/env python3
import sys


print("this is reducer 1")


current_term = None
term_data = []


def output():
    for doc_id, tf, positions in term_data:
        print(f"{current_term}\t{doc_id}\t{tf}\t{positions}")


for line in sys.stdin:
    term, doc_id, tf, positions = line.strip().split('\t')
   
    if term != current_term:
        if current_term is not None:
            output()
        current_term = term
        term_data = []
   
    term_data.append((doc_id, int(tf), positions))


if current_term is not None:
    output()