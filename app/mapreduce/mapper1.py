#!/usr/bin/env python3
import sys
import re
from collections import defaultdict


print("this is mapper 1")


def tokenize(text):
    # Tokenizing text to find words
    words = re.findall(r'\b\w[\w-]*\b', text.lower())
    return words


for line in sys.stdin:
    try:
        # Parsing tab-separated input
        doc_id, doc_title, text = line.strip().split('\t', 2)
        words = tokenize(text)
        term_positions = defaultdict(list)
       
        # Calculating word positions
        for position, word in enumerate(words):
            term_positions[word].append(position)
           
        for term, positions in term_positions.items():
            print(f"{term}\t{doc_id}\t{len(positions)}\t{','.join(map(str, positions))}")
    except Exception as e:
        # Skip malformed lines but log the error
        sys.stderr.write(f"ERROR processing line: {line}\n")
        continue