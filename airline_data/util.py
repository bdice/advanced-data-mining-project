import numpy as np
import os
from tqdm import tqdm


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


def hdfs_fn(job, filename):
    """Prepend a filename with the job's HDFS workspace directory path."""
    return os.path.join(job._project.config['hdfs_dir'], job._id, filename)


coupon_dtypes = {
    'ItinID': np.int64,
    'MktID': np.int64,
    'SeqNum': np.int8,
    'Coupons': np.int8,
    'Year': np.int16,
    'OriginAirportID': np.int16,
    'OriginAirportSeqID': np.int32,
    'OriginCityMarketID': np.int32,
    'Quarter': np.int8,
    'Origin': str,
    'OriginCountry': str,
    'OriginStateFips': np.int8,
    'OriginState': str,
    'OriginStateName': str,
    'OriginWac': np.int8,
    'DestAirportID': np.int16,
    'DestAirportSeqID': np.int32,
    'DestCityMarketID': np.int32,
    'Dest': str,
    'DestCountry': str,
    'DestStateFips': np.int8,
    'DestState': str,
    'DestStateName': str,
    'DestWac': np.int8,
    'Break': str,
    'CouponType': str,
    'TkCarrier': str,
    'OpCarrier': str,
    'RPCarrier': str,
    'Passengers': np.float32,
    'FareClass': str,
    'Distance': np.float32,
    'DistanceGroup': np.int8,
    'ItinGeoType': np.int8,
    'CouponGeoType': np.int8}


def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def get_community(paths, community_path):
    if len(community_path) == 3:
        left = tuple([*community_path[:-1], community_path[-1]-1])
        right = tuple([*community_path[:-1], community_path[-1]+1])
    else:
        left = tuple([*community_path, *(-1,)*(3-len(community_path))])
        right = tuple([*community_path[:-1], community_path[-1]+1, *(0,)*(3-len(community_path))])
    return paths[paths.path.between(left, right)]
