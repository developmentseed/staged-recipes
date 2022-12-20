import glob
import os
from os.path import join

import s3fs

from pangeo_forge_recipes.patterns import pattern_from_file_sequence

# from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe

url_base = 'noaa-cdr-microwave-brit-temp-pds/data/'

file_list = []
fs = s3fs.S3FileSystem(anon=True)


def is_nc(x):
    return x.endswith('.nc')


def add_s3(x):
    return 's3://' + x


years_folders = fs.ls(join(url_base))
years = list(map(lambda x: os.path.basename(x), years_folders))

sensor_list = ['N15', 'N16', 'N17', 'N18', 'AQUA', 'M02']

# psuedo code:
# iterate through sensor list, if sensor (e.g., N15) is in file_list
# iterate through year in years and create separate archive for each sensor

# work-in-progess implementing pseudo code ab
for year in years:
    file_list += sorted(filter(is_nc, map(add_s3, fs.ls(join(url_base, str(year)), detail=False))))
pattern = pattern_from_file_sequence(file_list, 'time', nitems_per_file=1)
for item in sensor_list:
    for name in glob.glob('*.nc'):
        if item in name:
            print(file_list)

# print(pattern)
# recipe = HDFReferenceRecipe(pattern, netcdf_storage_options={'anon': True})
