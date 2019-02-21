#!/usr/bin/env python
import signac
from tqdm import tqdm
import os
import urllib.request


class TqdmUpTo(tqdm):
    """Provides `update_to(n)` which uses `tqdm.update(delta_n)`."""

    def update_to(self, b=1, bsize=1, tsize=None):
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] remains unchanged.
        """
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)  # will also set self.n = b * bsize


if __name__ == '__main__':
    try:
        project = signac.get_project()
    except LookupError:
        project = signac.init_project('US_Flights')

    for year in range(1993, 2019):
        for quarter in range(1, 5):
            if year == 2018 and quarter == 4:
                continue
            job = project.open_job({'year': year, 'quarter': quarter})
            job.init()
            for field in ('Coupon', 'Market', 'Ticket'):
                filename = '{}_{}_{}.zip'.format(field, year, quarter)
                url = 'https://transtats.bts.gov/PREZIP/Origin_and_Destination_Survey_DB1B' + filename
                if job.isfile('{}.zip'.format(field)):
                    print('Already have {}-Q{} {} data.'.format(year, quarter, field))
                else:
                    print('Fetching {}-Q{} {} data...'.format(year, quarter, field))
                    try:
                        with TqdmUpTo(unit='B', unit_scale=True, miniters=1,
                                      desc=url.split('/')[-1]) as t:
                            urllib.request.urlretrieve(url, job.fn('{}.zip'.format(field)),
                                                       reporthook=t.update_to)
                    except Exception as e:
                        print(e)
