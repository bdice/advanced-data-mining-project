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
@Project.post.isfile('first_order_pagerank.txt')
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
    pageranks.sort_values('PageRank', ascending=False).to_csv(
        job.fn('first_order_pagerank.txt'), header=False)
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


@Project.operation
@Project.pre.after(generate_hon)
@Project.post.isfile('hon_infomap.txt')
def hon_prepare_infomap(job):
    import json
    from collections import OrderedDict

    vertices = set()
    state_nodes = OrderedDict()
    edges = set()
    with open(job.fn('hon_network.txt'), 'r') as hon_network:
        # Line format is [FromNode],[ToNode],weight
        # where [FromNode] and [ToNode] have format current_node|past0.past1...
        from_nodes, to_nodes, weights = \
            zip(*map(lambda line: line.strip().split(','),
                     hon_network.readlines()))

    for from_node, to_node in zip(from_nodes, to_nodes):
        physical_name_from = from_node.split('|')[0]
        physical_name_to = to_node.split('|')[0]
        vertices.add(physical_name_to)
        vertices.add(physical_name_from)

    keys = list(range(1, len(vertices)+1))
    vertices = OrderedDict(zip(vertices, keys))

    # Save the mapping to the arbitrary keys used for InfoMap compatibility
    with open(job.fn('hon_infomap_vertex_keys.json'), 'w') as f:
        json.dump(vertices, f)
    with open(job.fn('hon_infomap_key_vertices.json'), 'w') as f:
        json.dump({v: k for k, v in vertices.items()}, f)

    # Now, go through and get unique ID's for state nodes, use dict to get their
    # matching physical node
    unique_ID = 1
    for from_node, to_node in zip(from_nodes, to_nodes):
        physical_name_from = from_node.split('|')[0]
        physical_name_to = to_node.split('|')[0]

        physical_ID_from = vertices[physical_name_from]
        physical_ID_to = vertices[physical_name_to]

        if from_node not in state_nodes:
            state_nodes[from_node] = (unique_ID, physical_ID_from)
            unique_ID += 1
        if to_node not in state_nodes:
            state_nodes[to_node] = (unique_ID, physical_ID_to)
            unique_ID += 1

    # Finally, get edge weights
    for from_node, to_node, weight in zip(from_nodes, to_nodes, weights):
        edges.add((state_nodes[from_node][0], state_nodes[to_node][0], weight))

    with open(job.fn('hon_infomap.txt'), 'w') as f:
        f.write("*Vertices {}\n".format(len(vertices)))
        for vertex in vertices:
            f.write("{} {}\n".format(str(vertices[vertex]), vertex))
        f.write("*States\n")
        for node in state_nodes:
            f.write("{} {} {}\n".format(str(state_nodes[node][0]),
                                        str(state_nodes[node][1]),
                                        str(node)))
        f.write("*Links\n")
        for edge in edges:
            f.write('{} {} {}\n'.format(edge[0], edge[1], edge[2]))


@Project.operation
@Project.pre.after(hon_prepare_infomap)
@Project.post.isfile('hon_infomap.clu')
@Project.post.isfile('hon_infomap.tree')
@cmd
def hon_infomap(job):
    return ('../infomap/Infomap --clu --tree -vv -d -i states '
            '{input} {output}').format(
                input=job.fn('hon_infomap.txt'),
                output=job.ws)


@Project.operation
@Project.pre.after(hon_infomap)
@Project.post.isfile('hon_communities.csv')
def plot_hon_communities(job):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.colors as mcolors
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt
    import networkx as nx
    import numpy as np
    import os
    import pandas as pd
    import cartopy.crs as ccrs
    import cartopy.io.shapereader as shpreader
    from util import get_community

    paths = pd.read_csv(job.fn('hon_infomap.tree'), sep=' ', header=None,
                        skiprows=2, names=['path', 'flow', 'ID', 'node'])
    paths['path'] = paths['path'].apply(lambda x: tuple(map(int, x.split(':'))))
    iatas = pd.read_csv(job.fn('airport_codes.csv'))
    iatas['ID'] = pd.to_numeric(iatas['ID'])
    iatas = iatas.set_index('ID')
    airports = pd.merge(iatas, fetch_geodata(), left_on='IATA', right_index=True)
    paths = pd.merge(paths, airports, left_on='ID', right_index=True)
    paths = paths.sort_values('path')

    output_directory = job.fn('hon_community_plots')
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    def plot_community(paths, community=None):
        print('Generating plot...')
        fig = plt.figure(figsize=(6, 4), dpi=400)
        ax = fig.add_axes([0, 0, 1, 1], projection=ccrs.LambertConformal())
        ax.set_extent([-128, -64, 22, 49], ccrs.Geodetic())

        shapename = 'admin_1_states_provinces_shp'
        states_shp = shpreader.natural_earth(resolution='110m',
                                             category='cultural', name=shapename)

        ax.background_patch.set_visible(False)
        ax.outline_patch.set_visible(False)
        plt.title('Communit{} from Higher-Order Network ({}Q{})'.format(
            'ies' if community is None else 'y ' + str(community),
            job.sp.year, job.sp.quarter))

        def colorize_state(geometry):
            facecolor = (0.9375, 0.9375, 0.8594)
            return {'facecolor': facecolor, 'edgecolor': 'black'}

        ax.add_geometries(
            shpreader.Reader(states_shp).geometries(),
            ccrs.PlateCarree(),
            styler=colorize_state)

        xs = paths.lon.values
        ys = paths.lat.values
        colors = paths.flow.values
        sizes = 10000*paths.flow.values


        dots = ax.scatter(xs, ys, transform=ccrs.PlateCarree(), c=colors,
                          s=sizes, alpha=0.8, zorder=10,
                          norm=mcolors.LogNorm(vmin=1e-4, vmax=1e-1),
                          cmap='viridis')

        top_n = paths.sort_values('flow', ascending=False).head(20).index
        for row in paths.loc[top_n].itertuples():
            ax.annotate(row.IATA, (row.lon, row.lat),
                        xycoords=ccrs.PlateCarree()._as_mpl_transform(ax),
                        fontsize=40*row.flow**0.2, zorder=11, ha='center', va='center')
        cbax = fig.add_axes([0.9, 0.1, 0.03, 0.8])
        cbar = plt.colorbar(dots, cax=cbax)
        cbar.set_label('Flow through node', rotation=90)
        plt.savefig(os.path.join(
            output_directory,
            'hon_community_{}.png'.format(','.join(map(str, community)))))
        plt.close()

    for community in paths.path.apply(lambda x: x[:2]).unique():
        data = get_community(paths, community)
        flow = sum(data.flow)
        if flow > 5e-3:  # arbitrary cutoff
            print('Plotting {}Q{} community {}, flow {:.3g}'.format(
                job.sp.year, job.sp.quarter, community, flow))
            plot_community(data, community=community)

    paths.to_csv(job.fn('hon_communities.csv'))


@Project.operation
@Project.pre.after(combine_edgefile)
@Project.post.isfile('first_order_infomap.txt')
def first_order_prepare_infomap(job):
    import pandas as pd
    import json
    from collections import OrderedDict

    state_nodes = OrderedDict()
    edges = set()
    first_order_network = pd.read_csv(
        job.fn('all_edges.tsv'), sep='\t', header=None,
        names=['source', 'dest', 'weight'])

    unique_ids = list(map(int, sorted(
        set(first_order_network['source'].unique()).union(
            set(first_order_network['dest'].unique())))))

    keys = list(range(1, len(unique_ids)+1))
    vertices = OrderedDict({k: v for (k, v) in zip(unique_ids, keys)})

    # Save the mapping to the arbitrary keys used for InfoMap compatibility
    with open(job.fn('first_order_infomap_vertex_keys.json'), 'w') as f:
        json.dump(vertices, f)
    with open(job.fn('first_order_infomap_key_vertices.json'), 'w') as f:
        json.dump({v: k for k, v in vertices.items()}, f)

    with open(job.fn('first_order_infomap.txt'), 'w') as f:
        # Written in Pajek format for InfoMap
        f.write('*Vertices {}\n'.format(len(vertices)))
        for k in sorted(vertices.keys(), key=lambda k: vertices[k]):
            f.write('{} "{}"\n'.format(vertices[k], k))
        f.write('*Edges {}\n'.format(len(first_order_network)))
        for row in first_order_network.itertuples():
            f.write('{} {} {}\n'.format(vertices[row.source], vertices[row.dest], row.weight))


@Project.operation
@Project.pre.after(first_order_prepare_infomap)
@Project.post.isfile('first_order_infomap.clu')
@Project.post.isfile('first_order_infomap.tree')
@cmd
def first_order_infomap(job):
    return ('../infomap/Infomap --clu --tree -vv -d -i pajek '
            '{input} {output}').format(
                input=job.fn('first_order_infomap.txt'),
                output=job.ws)


@Project.operation
@Project.pre.after(first_order_infomap)
@Project.post.isfile('first_order_communities.csv')
def plot_first_order_communities(job):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.colors as mcolors
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt
    import networkx as nx
    import numpy as np
    import os
    import pandas as pd
    import cartopy.crs as ccrs
    import cartopy.io.shapereader as shpreader
    from util import get_community

    paths = pd.read_csv(job.fn('first_order_infomap.tree'), sep=' ',
                        header=None, skiprows=2,
                        names=['path', 'flow', 'ID', 'node'])
    paths['path'] = paths['path'].apply(lambda x: tuple(map(int, x.split(':'))))
    iatas = pd.read_csv(job.fn('airport_codes.csv'))
    iatas['ID'] = pd.to_numeric(iatas['ID'])
    iatas = iatas.set_index('ID')
    airports = pd.merge(iatas, fetch_geodata(), left_on='IATA', right_index=True)
    paths = pd.merge(paths, airports, left_on='ID', right_index=True)
    paths = paths.sort_values('path')

    output_directory = job.fn('first_order_community_plots')
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    def plot_community(paths, community=None):
        print('Generating plot...')
        fig = plt.figure(figsize=(6, 4), dpi=400)
        ax = fig.add_axes([0, 0, 1, 1], projection=ccrs.LambertConformal())
        ax.set_extent([-128, -64, 22, 49], ccrs.Geodetic())

        shapename = 'admin_1_states_provinces_shp'
        states_shp = shpreader.natural_earth(resolution='110m',
                                             category='cultural', name=shapename)

        ax.background_patch.set_visible(False)
        ax.outline_patch.set_visible(False)
        plt.title('Communit{} from First-Order Network ({}Q{})'.format(
            'ies' if community is None else 'y ' + str(community),
            job.sp.year, job.sp.quarter))

        def colorize_state(geometry):
            facecolor = (0.9375, 0.9375, 0.8594)
            return {'facecolor': facecolor, 'edgecolor': 'black'}

        ax.add_geometries(
            shpreader.Reader(states_shp).geometries(),
            ccrs.PlateCarree(),
            styler=colorize_state)

        xs = paths.lon.values
        ys = paths.lat.values
        colors = paths.flow.values
        sizes = 10000*paths.flow.values


        dots = ax.scatter(xs, ys, transform=ccrs.PlateCarree(), c=colors,
                          s=sizes, alpha=0.8, zorder=10,
                          norm=mcolors.LogNorm(vmin=1e-4, vmax=1e-1),
                          cmap='viridis')

        top_n = paths.sort_values('flow', ascending=False).head(20).index
        for row in paths.loc[top_n].itertuples():
            ax.annotate(row.IATA, (row.lon, row.lat),
                        xycoords=ccrs.PlateCarree()._as_mpl_transform(ax),
                        fontsize=40*row.flow**0.2, zorder=11, ha='center', va='center')
        cbax = fig.add_axes([0.9, 0.1, 0.03, 0.8])
        cbar = plt.colorbar(dots, cax=cbax)
        cbar.set_label('Flow through node', rotation=90)
        plt.savefig(os.path.join(
            output_directory,
            'first_order_community_{}.png'.format(','.join(map(str, community)))))
        plt.close()

    for community in paths.path.apply(lambda x: x[:1]).unique():
        data = get_community(paths, community)
        flow = sum(data.flow)
        if flow > 5e-3:  # arbitrary cutoff
            print('Plotting {}Q{} community {}, flow {:.3g}'.format(
                job.sp.year, job.sp.quarter, community, flow))
            plot_community(data, community=community)

    paths.to_csv(job.fn('first_order_communities.csv'))


@Project.operation
@Project.pre.after(plot_hon_communities)
@Project.post.isfile('hon_top_communities.png')
def plot_hon_top_communities(job):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.colors as mcolors
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt
    import networkx as nx
    import numpy as np
    import os
    import pandas as pd
    import cartopy.crs as ccrs
    import cartopy.io.shapereader as shpreader
    from tqdm import tqdm
    from util import get_community

    communities = pd.read_csv(job.fn('hon_communities.csv'), usecols=['path', 'flow', 'ID', 'IATA', 'lon', 'lat'])
    communities.path = communities.path.apply(lambda x: tuple(map(int, x[1:-1].split(', '))))

    def plot_communities(all_communities, top_count=5):
        print('Generating plot...')
        fig = plt.figure(figsize=(6, 4), dpi=400)
        ax = fig.add_axes([0, 0, 1, 1], projection=ccrs.LambertConformal())
        ax.set_extent([-128, -64, 22, 49], ccrs.Geodetic())

        shapename = 'admin_1_states_provinces_shp'
        states_shp = shpreader.natural_earth(resolution='110m',
                                             category='cultural', name=shapename)

        ax.background_patch.set_visible(False)
        ax.outline_patch.set_visible(False)
        plt.title('Top {} Communities from Higher-Order Network ({}Q{})'.format(
            top_count, job.sp.year, job.sp.quarter))

        def colorize_state(geometry):
            facecolor = (0.9375, 0.9375, 0.8594)
            return {'facecolor': facecolor, 'edgecolor': 'black'}

        ax.add_geometries(
            shpreader.Reader(states_shp).geometries(),
            ccrs.PlateCarree(),
            styler=colorize_state)

        cmap = matplotlib.cm.get_cmap('tab10')
        marked_iatas = set()
        for i, paths in enumerate(
                sorted(all_communities, key=lambda x: -sum(x.flow))[:top_count]):
            xs = paths.lon.values
            ys = paths.lat.values
            colors = (cmap(i),) * len(xs)
            sizes = 10000*paths.flow.values
            dots = ax.scatter(xs, ys, transform=ccrs.PlateCarree(), c=colors, s=sizes, alpha=0.8, zorder=10)

            top_n = paths.sort_values('flow', ascending=False).head(10).index
            for row in paths.loc[top_n].itertuples():
                if row.IATA not in marked_iatas:
                    ax.annotate(row.IATA, (row.lon, row.lat), xycoords=ccrs.PlateCarree()._as_mpl_transform(ax),
                                fontsize=10, #40*row.flow**0.2,
                                zorder=11, ha='center', va='center')
                    marked_iatas.add(row.IATA)
        plt.savefig(job.fn('hon_top_communities.png'))
        plt.close()

    all_communities = [get_community(communities, community) for community in tqdm(
        communities.path.apply(lambda x: x[:2]).unique())]

    plot_communities(all_communities, 5)


@Project.operation
@Project.pre.after(plot_hon_communities)
@Project.post.isfile('hon_community_carriers.json')
def plot_hon_community_carriers(job):
    import json
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import pandas as pd
    import os
    from util import get_community

    communities = pd.read_csv(
        job.fn('hon_communities.csv'),
        usecols=['path', 'flow', 'ID'])
    communities.path = communities.path.apply(lambda x: tuple(map(int, x[1:-1].split(', '))))

    carriers = pd.read_csv(
        job.fn('Coupon.csv'),
        usecols=['OriginAirportID', 'DestAirportID', 'TkCarrier'])

    output_directory = job.fn('hon_community_plots')
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    community_carriers = {}

    def plot_community_carriers(communities, carriers, community):
        print('Fetching community...')
        community_data = get_community(communities, community)
        print('Computing flows...')
        flows = community_data[['ID', 'flow']].groupby('ID').sum()
        print('Merging origin flows...')
        carrier_flows = pd.merge(carriers, flows, left_on='OriginAirportID', right_index=True)
        carrier_flows = carrier_flows.rename(index=str, columns={'flow': 'OriginFlow'})
        print('Merging destination flows...')
        carrier_flows = pd.merge(carrier_flows, flows, left_on='DestAirportID', right_index=True)
        carrier_flows = carrier_flows.rename(index=str, columns={'flow': 'DestFlow'})
        print('Computing total flows...')
        carrier_flows['TotalFlow'] = carrier_flows['OriginFlow'] + carrier_flows['DestFlow']
        carrier_flows = carrier_flows[['TkCarrier', 'TotalFlow']].groupby('TkCarrier').sum()
        carrier_flows = carrier_flows.sort_values('TotalFlow', ascending=False)
        print('Plotting results...')
        plt.figure(figsize=(6, 4), dpi=300)
        carrier_flows.TotalFlow.plot.pie(fontsize=20)
        plt.title('Carriers weighted by flow,\n{year}Q{quarter} Community {community}'.format(
            **job.sp, community=community), fontsize=26)
        plt.ylabel(None)
        plt.tight_layout()
        plt.savefig(os.path.join(
            output_directory,
            'hon_community_carriers_{}.png'.format(
                ','.join(map(str, community)))))
        plt.close()
        community_carriers[str(community)] = carrier_flows.to_dict()


    for community in communities.path.apply(lambda x: x[:2]).unique():
        data = get_community(communities, community)
        flow = sum(data.flow)
        if flow > 5e-3:  # arbitrary cutoff
            print('Plotting {}Q{} community {}, flow {:.3g}'.format(
                job.sp.year, job.sp.quarter, community, flow))
            plot_community_carriers(communities, carriers, community)

    with open(job.fn('hon_community_carriers.json'), 'w') as f:
        json.dump(community_carriers, f)


if __name__ == '__main__':
    Project().main()
