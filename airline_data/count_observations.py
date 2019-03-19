#!/usr/bin/env python
import signac

project = signac.get_project()
counts = {'Coupon.zip': 0, 'Market.zip': 0, 'Ticket.zip': 0}

for job in project:
    for fn in counts:
        counts[fn] += job.doc.get(fn, {'shape': (0, 0)})['shape'][0]
    print(counts)
