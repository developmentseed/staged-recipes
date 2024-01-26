import base64
import json
import os
from dataclasses import dataclass, field
from typing import MutableMapping

import apache_beam as beam
import pandas as pd
import requests
import zarr
from requests.auth import HTTPBasicAuth

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import OpenWithKerchunk, WriteCombinedReference, ConsolidateMetadata

ED_USERNAME = os.environ['EARTHDATA_USERNAME']
ED_PASSWORD = os.environ['EARTHDATA_PASSWORD']
earthdata_protocol = os.environ['PROTOCOL'] or 'https'

if earthdata_protocol not in ('https', 's3'):
    raise ValueError(f'Unknown ED_PROTOCOL: {earthdata_protocol}')

CREDENTIALS_API = 'https://data.gesdisc.earthdata.nasa.gov/s3credentials'
SHORT_NAME = 'GPM_3IMERGDF.07'
CONCAT_DIMS = ['time']
IDENTICAL_DIMS = ['lat', 'lon']

# 2023/07/3B-DAY.MS.MRG.3IMERG.20230731
dates = [
    d.to_pydatetime().strftime('%Y/%m/3B-DAY.MS.MRG.3IMERG.%Y%m%d')
    for d in pd.date_range('2000-06-01', '2001-01-01', freq='D')
]


def make_filename(time):
    if earthdata_protocol == 'https':
        # https://data.gesdisc.earthdata.nasa.gov/data/GPM_L3/GPM_3IMERGDF.07/2023/07/3B-DAY.MS.MRG.3IMERG.20230731-S000000-E235959.V07B.nc4
        base_url = f'https://data.gesdisc.earthdata.nasa.gov/data/GPM_L3/{SHORT_NAME}/'
    else:
        base_url = f's3://gesdisc-cumulus-prod-protected/GPM_L3/{SHORT_NAME}/'
    return f'{base_url}{time}-S000000-E235959.V07B.nc4'


concat_dim = ConcatDim('time', dates, nitems_per_file=1)
pattern = FilePattern(make_filename, concat_dim)


def get_earthdata_token(username, password):
    # URL for the Earthdata login endpoint
    login_url = 'https://urs.earthdata.nasa.gov/api/users/token'
    auth = HTTPBasicAuth(username, password)

    # Request a new token
    response = requests.get(f'{login_url}s', auth=auth)

    # Check if the request was successful
    if response.status_code == 200:
        if len(response.json()) == 0:
            # create new token
            response = requests.post(login_url, auth=auth)
            if response.status_code == 200:
                token = response.json()['access_token']
            else:
                raise Exception('Error: Unable to generate Earthdata token.')
        else:
            # Token is usually in the response's JSON data
            token = response.json()[0]['access_token']
        return token
    else:
        raise Exception('Error: Unable to retrieve Earthdata token.')


def get_s3_creds(username, password, credentials_api=CREDENTIALS_API):
    login_resp = requests.get(CREDENTIALS_API, allow_redirects=False)
    login_resp.raise_for_status()

    encoded_auth = base64.b64encode(f'{username}:{password}'.encode('ascii'))
    auth_redirect = requests.post(
        login_resp.headers['location'],
        data={'credentials': encoded_auth},
        headers={'Origin': credentials_api},
        allow_redirects=False,
    )
    auth_redirect.raise_for_status()

    final = requests.get(auth_redirect.headers['location'], allow_redirects=False)

    results = requests.get(CREDENTIALS_API, cookies={'accessToken': final.cookies['accessToken']})
    results.raise_for_status()

    creds = json.loads(results.content)
    return {
        'key': creds['accessKeyId'],
        'secret': creds['secretAccessKey'],
        'token': creds['sessionToken'],
        'anon': False,
    }


def earthdata_auth(username: str, password: str):
    if earthdata_protocol == 's3':
        return get_s3_creds(username, password)
    else:
        token = get_earthdata_token(username, password)
        return {'headers': {'Authorization': f'Bearer {token}'}}


fsspec_open_kwargs = earthdata_auth(ED_USERNAME, ED_PASSWORD)
remote_and_target_auth_options = {
    'key': os.environ["S3_DEFAULT_AWS_ACCESS_KEY_ID"],
    'secret': os.environ["S3_DEFAULT_AWS_SECRET_ACCESS_KEY"],
    "anon": False,
    'client_kwargs': {
        'region_name': 'us-west-2'
    }
}


def test_ds(store: zarr.hierarchy.Group) -> zarr.hierarchy.Group:
    import fsspec
    #fs = fsspec.filesystem('s3', **remote_and_target_auth_options)
    #import pdb; pdb.set_trace()
    assert isinstance(store, zarr.hierarchy.Group)
    assert isinstance(store._store, zarr.storage.ConsolidatedMetadataStore)
    assert isinstance(store._chunk_store, zarr.storage.FSStore)
    ref_path = store._chunk_store.fs.storage_options['fo']
    mapper = fsspec.get_mapper("reference://", fo=ref_path)
    zarr_group = zarr.open_consolidated(mapper)

    print(f"Group path: {zarr_group.path}")
    print(f"Number of arrays: {len(zarr_group)}")
    print(f"Number of subgroups: {len(list(zarr_group.groups()))}")

    for array_name in zarr_group.array_keys():
        array = zarr_group[array_name]
        print(f"\nArray name: {array_name}")
        print(f"Shape: {array.shape}")
        print(f"Data type: {array.dtype}")
        print(f"Chunk size: {array.chunks}")

        # Print array attributes if any
        if array.attrs:
            print("Attributes:")
            for attr, value in array.attrs.items():
                print(f"  {attr}: {value}")

    for subgroup in zarr_group.groups():
        print(f"Subgroup: {subgroup}")

    return store


def consolidate_metadata(store: MutableMapping, fsspec_kwargs: dict) -> zarr.hierarchy.Group:
    """Consolidate metadata for a Zarr store or Kerchunk reference

    :param store: Input Store for Zarr or Kerchunk reference
    :type store: MutableMapping
    :param fsspec_kwargs: all optional fsspec kwargs
    :type fsspec_kwargs: dict
    :return: Output Store
    :rtype: MutableMapping
    """
    import zarr
    import fsspec
    from fsspec.implementations.reference import ReferenceFileSystem

    if isinstance(store, fsspec.FSMap) and isinstance(store.fs, ReferenceFileSystem):
        ref_path = store.fs.storage_args[0]
        path = fsspec.get_mapper("reference://", fo=ref_path, **fsspec_kwargs)
    if isinstance(store, zarr.storage.FSStore):
        path = store.path

    zc = zarr.consolidate_metadata(path)
    return zc


@dataclass
class ConsolidateMetadataV2(beam.PTransform):
    """Calls Zarr Python consolidate_metadata on an existing Zarr store or Kerchunk reference
    (https://zarr.readthedocs.io/en/stable/_modules/zarr/convenience.html#consolidate_metadata)

    :param fsspec_kwargs: Additional kwargs to pass to ``fsspec.get_mapper``
    """
    fsspec_kwargs: dict = field(default_factory=dict)

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(consolidate_metadata, fsspec_kwargs=self.fsspec_kwargs)


recipe = (
    beam.Create(pattern.items())
    | OpenWithKerchunk(
        remote_protocol=earthdata_protocol,
        file_type=pattern.file_type,
        # lat/lon are around 5k, this is the best option for forcing kerchunk to inline them
        inline_threshold=6000,
        storage_options=fsspec_open_kwargs,
    )
    | WriteCombinedReference(
        concat_dims=CONCAT_DIMS,
        identical_dims=IDENTICAL_DIMS,
        store_name=SHORT_NAME,
    )
    | ConsolidateMetadataV2(fsspec_kwargs=remote_and_target_auth_options)
    | "Test dataset" >> beam.Map(test_ds)
)
