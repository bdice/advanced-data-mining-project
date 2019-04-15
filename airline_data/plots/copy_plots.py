#!/usr/bin/env python
import os
import signac
import shutil
from tqdm import tqdm

project = signac.get_project()

for plot_type in ('1st_order', 'hon'):
    if not os.path.exists(plot_type):
        os.makedirs(plot_type)
    for job in tqdm(sorted(project, key=lambda job: (job.sp.year, job.sp.quarter))):
        source_fn = job.fn('pr_{}.png'.format(plot_type))
        if os.path.exists(source_fn):
            shutil.copyfile(source_fn, os.path.join(
                plot_type, '{}Q{}.png'.format(job.sp.year, job.sp.quarter)))
