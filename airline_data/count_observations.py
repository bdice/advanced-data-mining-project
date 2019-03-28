#!/usr/bin/env python
import signac

project = signac.get_project()
counts = {'Coupon': 0}
sizes = {'Coupon': 0}


def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


for job in project:
    for fn in counts:
        counts[fn] += job.doc.get(fn, {'shape': (0, 0)})['shape'][0]
    for fn in sizes:
        sizes[fn] += job.doc.get(fn, {'file_size': 0})['file_size']

print(counts)
for fn in sizes:
    print(fn, sizeof_fmt(sizes[fn]))
