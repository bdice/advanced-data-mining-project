#!/usr/bin/env python
import signac
from util import sizeof_fmt


project = signac.get_project()
counts = {'Coupon': 0}
sizes = {'Coupon': 0}


for job in project:
    for fn in counts:
        counts[fn] += job.doc.get(fn, {'shape': (0, 0)})['shape'][0]
    for fn in sizes:
        sizes[fn] += job.doc.get(fn, {'file_size': 0})['file_size']

print(counts)
for fn in sizes:
    print(fn, sizeof_fmt(sizes[fn]))
