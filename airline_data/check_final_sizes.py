from __future__ import print_function
import os
import signac
from util import sizeof_fmt

project = signac.get_project()

for job in sorted(project, key=lambda job: [job.sp.year, job.sp.quarter]):
    hon_size = os.stat(job.fn('hon_itineraries.txt')).st_size
    print(job._id, job.sp.year, job.sp.quarter, hon_size)
