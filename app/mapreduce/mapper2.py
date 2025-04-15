#!/usr/bin/env python3
import sys


print("this is mapper 2")


# Identity mapper - just pass through the data
for line in sys.stdin:
    print(line.strip())