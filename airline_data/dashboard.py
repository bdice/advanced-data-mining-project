#!/usr/bin/env python3
# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from signac_dashboard import Dashboard
from signac_dashboard.modules import StatepointList, ImageViewer


class AirlineDashboard(Dashboard):
    def job_title(self, job):
        return '{year}Q{quarter}'.format(**job.sp)


if __name__ == '__main__':
    modules = []
    modules.append(ImageViewer('HON PageRank', img_globs=['pr_hon.png']))
    modules.append(ImageViewer('First Order PageRank', img_globs=['pr_1st_order.png']))
    modules.append(ImageViewer('HON Communities', img_globs=['hon_community_plots/hon_community_*.png']))
    AirlineDashboard(modules=modules).main()
