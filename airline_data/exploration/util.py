import os


def hdfs_fn(job, filename):
    """Prepend a filename with the job's HDFS workspace directory path."""
    return os.path.join(job._project.config['hdfs_dir'], job._id, filename)


def fetch_geodata():
    import pandas as pd
    import signac
    project = signac.get_project()

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


def get_community(paths, community_path):
    if len(community_path) == 3:
        left = tuple([*community_path[:-1], community_path[-1]-1])
        right = tuple([*community_path[:-1], community_path[-1]+1])
    else:
        left = tuple([*community_path, *(-1,)*(3-len(community_path))])
        right = tuple([*community_path[:-1], community_path[-1]+1, *(0,)*(3-len(community_path))])
    return paths[paths.path.between(left, right)]