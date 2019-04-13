#!/usr/bin/env python
import os
import signac
import shutil
from tqdm import tqdm

project = signac.get_project()

os.makedirs('1st_order', exist_ok=True)

for job in tqdm(sorted(project, key=lambda job: (job.sp.year, job.sp.quarter))):
    shutil.copyfile(job.fn('pr_1st_order.png'),
                    os.path.join('1st_order', '{}Q{}.png'.format(job.sp.year, job.sp.quarter)))
