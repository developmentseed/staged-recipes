# import glob
import os
from os.path import join

import s3fs

# from pangeo_forge_recipes.patterns import pattern_from_file_sequence

# from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe


def is_nc(x):
    return x.endswith('.nc')


def add_s3(x):
    return 's3://' + x


# sensor_list = ['N15', 'N16', 'N17', 'N18', 'AQUA', 'M02']

# work-in-progess implementing pseudo code above
url_base = 'noaa-cdr-microwave-brit-temp-pds/data/'
fs = s3fs.S3FileSystem(anon=True)

years_folders = fs.ls(join(url_base))
years = list(map(lambda x: os.path.basename(x), years_folders))

file_list = []

# WiP


def add_sensor(sensor_name):
    sensors_list = fs.find(url_base)
    for sensor in sensors_list:
        sensor_name = sensor.split('_')[4]
        return 'NESDIS-STAR_FCDR-GRID_AMSU-A_V01R00_' + sensor_name


# To Do:
# add sensor to URL,

# psuedo code:
# iterate through sensor list, if sensor (e.g., N15) is in file_list
# iterate through year in years and create separate archive for each sensor


# for sensor in sensors:
#    file_list += sorted(filter(is_nc, map(add_s3, fs.ls(join(url_base, str(year)), detail=False))))
#    for file in file_list:
#        sensor = file.split('_')[4]
#        for sensor in file_list:
#            pattern = pattern_from_file_sequence(file_list, 'time', nitems_per_file=1)
#            print(pattern)

# file_list = []
# for year in years:
#    file_list += sorted(filter(is_nc, map(add_s3, fs.ls(join(url_base, str(year)), detail=False))))
#    for file in file_list:
#        sensor = file.split('_')[4]
#        for sensor in file_list:
#            pattern = pattern_from_file_sequence(file_list, 'time', nitems_per_file=1)
#            print(pattern)


#     pattern = pattern_from_file_sequence(file_list, 'time', nitems_per_file=1)

# recipe = HDFReferenceRecipe(pattern, netcdf_storage_options={'anon': True})
