#!/usr/bin/env python
from flow import FlowProject, cmd, with_job
import zipfile


FIELDS = ('Coupon', 'Market', 'Ticket')


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
    return all([field in job.doc and 'shape' in job.doc[field] for field in FIELDS])


@Project.label
def unzipped(job):
    return all([is_unzipped(job, field + '.zip') for field in FIELDS])


@Project.label
def labeled(job):
    return all([job.isfile(field + '.csv') for field in FIELDS])


@Project.label
def has_edges(job):
    return job.isfile('edges.tsv/_SUCCESS')


@Project.label
def has_itins(job):
    return job.isfile('hon_itineraries.txt/_SUCCESS')


def build_spark():
    import findspark
    findspark.init('/usr/hdp/current/spark2-client')
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
        if field in job.doc and 'shape' in job.doc[field]:
            continue
        fn = field + '.zip'
        print('Reading {}'.format(job.fn(fn)))
        df = pd.read_csv(job.fn(fn))
        print('Shape: {}'.format(df.shape))
        job.doc[field] = {'shape': df.shape}


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
@Project.pre.after(unzipped)
@Project.post(labeled)
@with_job
@cmd
def label_data(job):
    return '; '.join(['mv -v Origin_and_Destination_Survey_DB1B{field}_{year}_{quarter}.csv {field}.csv'.format(
        field=field, **job.sp) for field in FIELDS])


@Project.operation
@Project.pre.after(labeled)
@Project.post(has_edges)
def extract_edges(job):
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
@Project.pre.after(labeled)
@Project.post(has_itins)
def extract_itineraries(job):
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


if __name__ == '__main__':
    Project().main()
