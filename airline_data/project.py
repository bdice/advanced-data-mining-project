#!/usr/bin/env python
from __future__ import print_function
from flow import FlowProject, cmd, with_job
import zipfile
from contextlib import contextmanager


#FIELDS = ('Coupon', 'Market', 'Ticket')
FIELDS = ('Coupon',)


class Project(FlowProject):
    pass


def valid_zip(job, filename):
    return job.isfile(filename) and zipfile.is_zipfile(job.fn(filename))


def is_unzipped(job, filename):
    return all([job.isfile(f) for f in zipfile.ZipFile(job.fn(filename)).namelist()])


@Project.label
def valid_data(job):
    return all([valid_zip(job, field + '.zip') for field in FIELDS])


def has_readme(job, filename):
    return valid_zip(job, filename) and \
        'readme.html' in zipfile.ZipFile(job.fn(filename)).namelist()


@Project.label
def has_readmes(job):
    return any([has_readme(job, field + '.zip') for field in FIELDS])


@Project.label
def has_counts(job):
    return all([field in job.doc and \
                'shape' in job.doc[field] and \
                'file_size' in job.doc[field] for field in FIELDS])


@Project.label
def unzipped(job):
    return all([is_unzipped(job, field + '.zip') for field in FIELDS])


@Project.label
def labeled(job):
    return all([job.isfile(field + '.csv') for field in FIELDS])


@Project.label
def has_IATA(job):
    return job.isfile('airport_codes.csv')


@Project.label
def has_edges(job):
    return job.isfile('edges.tsv/_SUCCESS')


@Project.label
def has_edgefile(job):
    return job.isfile('all_edges.tsv')


@Project.label
def has_itins(job):
    return job.isfile('hon_itineraries.txt/_SUCCESS')


@Project.label
def has_itineraryfile(job):
    return job.isfile('all_hon_itineraries.txt')


def find_spark():
    import findspark
    findspark.init('/usr/hdp/current/spark2-client')


@contextmanager
def build_spark():
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
    conf = (SparkConf().setMaster("yarn-client").setAppName("AirlineDataAnalysis")
            .set("spark.yarn.queue", "eecs598w19")
            .set("spark.executor.memory", "4g")
            .set("spark.executor.instances", "10")
            .set("spark.driver.memory", "4g")
            .set("spark.shuffle.service.enabled", "true")
            .set("spark.dynamicAllocation.enabled", "true")
            .set("spark.dynamicAllocation.minExecutors", "4")
            )
    with SparkSession.builder.config(conf=conf).getOrCreate() as spark:
        # Hide irrelevant warnings caused by workers running Python 2
        spark.sparkContext.setLogLevel("ERROR")
        yield spark


def fetch_geodata():
    import pandas as pd
    project = Project()

    if not project.isfile('airport_geodata.csv'):
        import urllib.request
        url = 'https://datahub.io/core/airport-codes/r/airport-codes.csv'
        urllib.request.urlretrieve(url, project.fn('airport_geodata.csv'))

    print('Reading airport geodata...')
    geodata = pd.read_csv(project.fn('airport_geodata.csv'))
    geodata = geodata.dropna(subset=['iata_code'])
    geodata = geodata[geodata['iso_country'] == 'US']
    geodata = geodata[['name', 'iata_code', 'coordinates']].set_index('iata_code')
    coords = geodata['coordinates'].str.split(', ', expand=True)
    geodata['lon'] = pd.to_numeric(coords[0])
    geodata['lat'] = pd.to_numeric(coords[1])
    geodata = geodata.drop(columns=['coordinates'])

    # Ignore airports with erroneous data near (0, 0)
    geodata = geodata[geodata.lon < -65]

    # Drop data for Alaska and Hawaii
    geodata = geodata[geodata.lon > -130]
    return geodata


@Project.operation
@Project.post(valid_data)
def fetch_data(job):
    from util import TqdmUpTo
    import urllib.request
    year = job.sp.year
    quarter = job.sp.quarter
    for field in FIELDS:
        filename = '{}_{}_{}.zip'.format(field, year, quarter)
        url = 'https://transtats.bts.gov/PREZIP/Origin_and_Destination_Survey_DB1B' + filename
        if valid_zip(job, field + '.zip'):
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


@Project.operation
@Project.pre.after(fetch_data)
@Project.pre(has_readmes)
@with_job
@cmd
def remove_readmes(job):
    return '; '.join(['zip -d {}.zip readme.html'.format(fn) for fn in FIELDS])


@Project.operation
@Project.pre.after(fetch_data)
@Project.pre.not_(has_readmes)
@Project.post(has_counts)
def determine_size(job):
    import pandas as pd
    for field in FIELDS:
        if field not in job.doc:
            job.doc[field] = {}

        # unzipped size
        if 'file_size' not in job.doc[field]:
            fn = field + '.zip'
            zf = zipfile.ZipFile(job.fn(fn))
            info = zf.infolist()
            size = info[0].file_size
            print('File size: {}'.format(size))
            job.doc[field]['file_size'] = size

        # CSV shape
        if 'shape' not in job.doc[field]:
            fn = field + '.zip'
            print('Reading {}'.format(job.fn(fn)))
            shape = [0, 0]
            for chunk in pd.read_csv(job.fn(fn), chunksize=100000):
                chunk_shape = chunk.shape
                shape[0] += chunk_shape[0]
                shape[1] = max(shape[1], chunk_shape[1])
            print('Shape: {}'.format(shape))
            job.doc[field]['shape'] = shape


@Project.operation
@Project.pre.after(fetch_data)
@Project.pre.not_(has_readmes)
@Project.pre.not_(labeled)
@Project.post(unzipped)
@with_job
@cmd
def unzip_data(job):
    return '; '.join(['unzip -o -DD {}.zip'.format(fn) for fn in FIELDS])


@Project.operation
@Project.pre.after(unzip_data)
@Project.post(labeled)
@with_job
@cmd
def label_data(job):
    return '; '.join(['mv -v Origin_and_Destination_Survey_DB1B{field}_{year}_{quarter}.csv {field}.csv'.format(
        field=field, **job.sp) for field in FIELDS])


@Project.operation
@Project.pre.after(label_data)
@Project.post(has_IATA)
def get_IATA(job):
    import numpy as np
    import pandas as pd
    from tqdm import tqdm
    from util import coupon_dtypes
    cols = ['OriginAirportID', 'Origin', 'DestAirportID', 'Dest']
    dtypes = {c: coupon_dtypes[c] for c in cols}
    chunksize = 100000
    reader = pd.read_csv(job.fn('Coupon.csv'), usecols=dtypes.keys(), dtype=dtypes, chunksize=chunksize)
    airport_codes = {}
    for chunk in tqdm(reader, total=int(np.ceil(job.doc['Coupon']['shape'][0]/chunksize))):
        for i, origin_id, origin_iata, dest_id, dest_iata in chunk.itertuples():
            if origin_id not in airport_codes:
                airport_codes[origin_id] = origin_iata
            if dest_id not in airport_codes:
                airport_codes[dest_id] = dest_iata
    airport_df = pd.DataFrame.from_dict(airport_codes, orient='index').sort_index()
    airport_df.index.name = 'ID'
    airport_df.columns = ['IATA']
    airport_df.to_csv(job.fn('airport_codes.csv'))


@Project.operation
@Project.pre.after(label_data)
@Project.post(has_edges)
def extract_edges(job):
    find_spark()
    from util import hdfs_fn
    from pyspark.sql.functions import count

    def make_line(row):
        return '{}\t{}\t{}'.format(row.OriginAirportID, row.DestAirportID, row.weight)

    with build_spark() as spark:
        df = spark.read.csv(hdfs_fn(job, 'Coupon.csv'), header=True, inferSchema=True)
        col_names = ['ItinID', 'OriginAirportID', 'DestAirportID']
        df_network = df[col_names].repartition('OriginAirportID')
        edges = df_network.groupby(['OriginAirportID', 'DestAirportID']).agg(count('ItinID').alias('weight'))
        edges.rdd.map(make_line).saveAsTextFile(hdfs_fn(job, 'edges.tsv'))


@Project.operation
@Project.pre.after(extract_edges)
@Project.post(has_edgefile)
@with_job
@cmd
def combine_edgefile(job):
    return '; '.join(['echo "Combining edges for job {}..."'.format(job._id),
                      'cat edges.tsv/part-* | sort > all_edges.tsv'])


@Project.operation
@Project.pre.after(label_data)
@Project.post(has_itins)
def extract_itineraries(job):
    find_spark()
    from util import hdfs_fn
    from pyspark.sql.functions import collect_list, first, last

    def make_line(row):
        return '{} {} {} {} {}'.format(
            row.ItinID, row.FirstAirportID,
            ' '.join(map(str, row.OriginAirportIDs)),
            row.LastAirportID, row.LastAirportID)

    with build_spark() as spark:
        df = spark.read.csv(hdfs_fn(job, 'Coupon.csv'), header=True, inferSchema=True)
        col_names = ['ItinID', 'SeqNum', 'OriginAirportID', 'Origin', 'DestAirportID', 'Dest']
        df_network = df[col_names].repartition('ItinID').sort(['ItinID', 'SeqNum'])
        itins = df_network.groupby(['ItinID']).agg(
            first('OriginAirportID').alias('FirstAirportID'),
            collect_list('OriginAirportID').alias('OriginAirportIDs'),
            last('DestAirportID').alias('LastAirportID'))
        itins.rdd.map(make_line).saveAsTextFile(hdfs_fn(job, 'hon_itineraries.txt'))


@Project.operation
@Project.pre.after(extract_itineraries)
@Project.post(has_itineraryfile)
@with_job
@cmd
def combine_itineraries(job):
    return '; '.join(['echo "Combining itineraries for job {}..."'.format(job._id),
                      'cat hon_itineraries.txt/part-* | sort > all_hon_itineraries.txt'])


@Project.operation
@Project.pre.after(combine_edgefile)
@Project.post.isfile('pr_1st_order.png')
def plot_first_order_pagerank(job):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.colors as mcolors
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt
    import networkx as nx
    import numpy as np
    import pandas as pd
    import cartopy.crs as ccrs
    import cartopy.io.shapereader as shpreader

    print('Reading edge list...')
    G = nx.read_weighted_edgelist(job.fn('all_edges.tsv'), create_using=nx.DiGraph())
    iatas = pd.read_csv(job.fn('airport_codes.csv'))
    iatas['ID'] = pd.to_numeric(iatas['ID'])
    iatas = iatas.set_index('ID')
    pageranks = pd.DataFrame.from_dict(nx.pagerank(G), orient='index')
    pageranks.columns = ['PageRank']
    pageranks.index = pd.to_numeric(pageranks.index)
    iatas = iatas.join(pageranks)

    airports = pd.merge(iatas, fetch_geodata(), left_on='IATA', right_index=True)
    airports['PageRankPercentile'] = airports['PageRank'].rank(pct=True)

    print('Generating plot...')
    fig = plt.figure(figsize=(6, 4), dpi=400)
    ax = fig.add_axes([0, 0, 1, 1], projection=ccrs.LambertConformal())
    ax.set_extent([-128, -64, 22, 49], ccrs.Geodetic())

    shapename = 'admin_1_states_provinces_shp'
    states_shp = shpreader.natural_earth(resolution='110m',
                                         category='cultural', name=shapename)

    ax.background_patch.set_visible(False)
    ax.outline_patch.set_visible(False)
    plt.title('PageRanks from First-Order Network ({}Q{})'.format(job.sp.year, job.sp.quarter))

    def colorize_state(geometry):
        facecolor = (0.9375, 0.9375, 0.8594)
        return {'facecolor': facecolor, 'edgecolor': 'black'}

    ax.add_geometries(
        shpreader.Reader(states_shp).geometries(),
        ccrs.PlateCarree(),
        styler=colorize_state)

    xs = airports.lon.values
    ys = airports.lat.values
    colors = airports.PageRank.values
    sizes = 10000*airports.PageRank.values

    dots = ax.scatter(xs, ys, transform=ccrs.PlateCarree(), c=colors, s=sizes, alpha=0.8, zorder=10,
                      norm=mcolors.LogNorm(vmin=1e-4, vmax=1e-1), cmap='viridis')

    for row in airports.itertuples():
        if row.PageRankPercentile > 0.90:
            ax.annotate(row.IATA, (row.lon, row.lat), xycoords=ccrs.PlateCarree()._as_mpl_transform(ax),
                        fontsize=40*row.PageRank**0.2, alpha=row.PageRankPercentile, zorder=11,
                        ha='center', va='center')
    cbax = fig.add_axes([0.9, 0.1, 0.03, 0.8])
    plt.colorbar(dots, cax=cbax)
    plt.savefig(job.fn('pr_1st_order.png'))
    plt.close()


@Project.operation
@Project.pre.after(combine_itineraries)
@Project.post.isfile('hon_rules.txt')
@Project.post.isfile('hon_network.txt')
def generate_hon(job):
    from pyHON import generate
    print('Generating HON for {year}Q{quarter}'.format(**job.sp))
    generate(job.fn('all_hon_itineraries.txt'),
             job.fn('hon_rules.txt'),
             job.fn('hon_network.txt'))


@Project.operation
@Project.pre.after(generate_hon)
@Project.post.isfile('hon_pagerank.txt')
def hon_pagerank(job):
    from pyHON import pagerank
    print('Computing HON PageRank for {year}Q{quarter}'.format(**job.sp))
    pagerank(job.fn('hon_network.txt'),
             job.fn('hon_pagerank.txt'))


@Project.operation
@Project.pre.after(hon_pagerank)
@Project.post.isfile('pr_hon.png')
def plot_hon_pagerank(job):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.colors as mcolors
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt
    import networkx as nx
    import numpy as np
    import pandas as pd
    import cartopy.crs as ccrs
    import cartopy.io.shapereader as shpreader

    print('Reading IATAs and PageRanks...')
    pageranks = pd.read_csv(job.fn('hon_pagerank.txt'), header=None, names=['ID', 'PageRank'])
    pageranks = pageranks.set_index('ID')
    iatas = pd.read_csv(job.fn('airport_codes.csv'))
    iatas['ID'] = pd.to_numeric(iatas['ID'])
    iatas = iatas.set_index('ID')
    iatas = pd.merge(iatas, pageranks, left_index=True, right_index=True)

    airports = pd.merge(iatas, fetch_geodata(), left_on='IATA', right_index=True)
    airports['PageRankPercentile'] = airports['PageRank'].rank(pct=True)

    print('Generating plot...')
    fig = plt.figure(figsize=(6, 4), dpi=400)
    ax = fig.add_axes([0, 0, 1, 1], projection=ccrs.LambertConformal())
    ax.set_extent([-128, -64, 22, 49], ccrs.Geodetic())

    shapename = 'admin_1_states_provinces_shp'
    states_shp = shpreader.natural_earth(resolution='110m',
                                         category='cultural', name=shapename)

    ax.background_patch.set_visible(False)
    ax.outline_patch.set_visible(False)
    plt.title('PageRanks from Higher-Order Network ({}Q{})'.format(job.sp.year, job.sp.quarter))

    def colorize_state(geometry):
        facecolor = (0.9375, 0.9375, 0.8594)
        return {'facecolor': facecolor, 'edgecolor': 'black'}

    ax.add_geometries(
        shpreader.Reader(states_shp).geometries(),
        ccrs.PlateCarree(),
        styler=colorize_state)

    xs = airports.lon.values
    ys = airports.lat.values
    colors = airports.PageRank.values
    sizes = 10000*airports.PageRank.values

    dots = ax.scatter(xs, ys, transform=ccrs.PlateCarree(), c=colors, s=sizes, alpha=0.8, zorder=10,
                      norm=mcolors.LogNorm(vmin=1e-4, vmax=1e-1), cmap='viridis')

    for row in airports.itertuples():
        if row.PageRankPercentile > 0.90:
            ax.annotate(row.IATA, (row.lon, row.lat), xycoords=ccrs.PlateCarree()._as_mpl_transform(ax),
                        fontsize=40*row.PageRank**0.2, alpha=row.PageRankPercentile, zorder=11,
                        ha='center', va='center')
    cbax = fig.add_axes([0.9, 0.1, 0.03, 0.8])
    plt.colorbar(dots, cax=cbax)
    plt.savefig(job.fn('pr_hon.png'))
    plt.close()


if __name__ == '__main__':
    Project().main()
