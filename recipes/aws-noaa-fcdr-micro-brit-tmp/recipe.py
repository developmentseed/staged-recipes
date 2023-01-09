# import csv
# import os
# from os.path import join

import boto3

# from pangeo_forge_recipes.patterns import pattern_from_file_sequence

# import s3fs


# from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe


def is_nc(x):
    return x.endswith('.nc')


def add_s3(x):
    return 's3://' + x


client = boto3.client('s3')
bucket_name = 'noaa-cdr-microwave-brit-temp-pds'

paginator = client.get_paginator('list_objects_v2')

# 'list_objects_v2': Returns some or all (up to 1000) of the objects in a bucket
# this could be a limiting factor

response_iterator = paginator.paginate(
    Bucket=bucket_name,
    Prefix='data',
)

# sensor_list = ['N15', 'N16', 'N17', 'N18', 'AQUA', 'M02']
# must also ignore files that don't include GRID in filename


# WORKS
for result in response_iterator:
    contents = result.get('Contents', [])
    # can implement filter here
    for f in contents:
        key = f['Key']
        # print(f's3://{bucket_name}/{key}')

        if f['Key'].split('_')[4] == 'N15':
            # years = f["Key"].split('/')[1]
            # print(years.count('2022'))

            matching_files = [f for f in contents]
            print(matching_files)

            # pattern_N15 = pattern_from_file_sequence(matching_files, 'time')
            # print(pattern_N15)


# pattern = pattern_from_file_sequence(file_list, 'time', nitems_per_file=1)
# recipe = HDFReferenceRecipe(pattern, netcdf_storage_options={'anon': True})


# work-in-progess old method

# url_base = 'noaa-cdr-microwave-brit-temp-pds/data/'
# fs = s3fs.S3FileSystem(anon=True)

# years_folders = fs.ls(join(url_base))
# years = list(map(lambda x: os.path.basename(x), years_folders))

# file_list = []

# def add_sensor(sensor_name):
#     sensors_list = fs.find(url_base)
#     for sensor in sensors_list:
#         sensor_name = sensor.split('_')[4]
#         return 'NESDIS-STAR_FCDR-GRID_AMSU-A_V01R00_' + sensor_name


# for year in years:
#   file_list += sorted(filter(is_nc, map(add_s3, fs.ls(join(url_base, str(year)), detail=False))))

#  # To Do:
#  # add add_sensor to URL (to retrieve only those with GRID in file name, iterate thru all sensors)

#     pattern = pattern_from_file_sequence(file_list, 'time', nitems_per_file=1)
#     print(pattern)

# psuedo code:
# iterate through sensor list, if sensor (e.g., N15) is in file_list
# iterate through year in years and create separate archive for each sensor
# want to ignore files that don't include GRID in filename
