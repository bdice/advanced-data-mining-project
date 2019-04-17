#!/usr/bin/env python3
# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from signac_dashboard import Dashboard
from signac_dashboard.modules import StatepointList, ImageViewer, FileList


class AirlineDashboard(Dashboard):
    def job_title(self, job):
        return '{year}Q{quarter}'.format(**job.sp)


if __name__ == '__main__':
    modules = []
    modules.append(ImageViewer('HON PageRank', img_globs=['pr_hon.png']))
    modules.append(ImageViewer('First-Order PageRank', img_globs=['pr_1st_order.png']))
    modules.append(ImageViewer('HON Communities', img_globs=['hon_community_plots/hon_community_*.png']))
    modules.append(ImageViewer('HON Top 9 Communities', img_globs=['hon_community_plots/hon_community_1,[1-9].png']))
    modules.append(ImageViewer('HON Overlaid 5 Communities', img_globs=['hon_top_communities.png']))
    modules.append(ImageViewer('First-Order Communities', img_globs=['first_order_community_plots/first_order_community_*.png']))
    modules.append(FileList())
    AirlineDashboard(modules=modules).main()
