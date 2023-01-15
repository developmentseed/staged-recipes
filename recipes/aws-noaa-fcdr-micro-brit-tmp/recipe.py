import boto3

from pangeo_forge_recipes.patterns import pattern_from_file_sequence

# from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe

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

# psuedo code:
# iterate through sensor list, if sensor (e.g., N15) is in file_list
# iterate through year in years and create separate archive for each sensor
# want to ignore files that don't include GRID in filename
# for now group ascending and descending together

for result in response_iterator:
    contents = result.get('Contents', [])
    # can implement filter here
    for f in contents:
        key = f['Key']
        # print(key)
        # look at only gridded files
        if '-GRID_' in key:
            # to iterate through yearly subfolders = key.split('/')[1]
            for key.split('/')[1] in key:
                if '_N15_' in key:
                    pattern_N15 = pattern_from_file_sequence(key, 'time')
                    print('pattern_N15')
                    print(pattern_N15)
                if '_N16_' in key:
                    pattern_N16 = pattern_from_file_sequence(key, 'time')
                    print('pattern_N16')
                    print(pattern_N16)
                if '_N17_' in key:
                    pattern_N17 = pattern_from_file_sequence(key, 'time')
                    print('pattern_N17')
                    print(pattern_N17)
                if '_N18_' in key:
                    pattern_N18 = pattern_from_file_sequence(key, 'time')
                    print('pattern_N18')
                    print(pattern_N18)
                if '_AQUA_' in key:
                    pattern_AQUA = pattern_from_file_sequence(key, 'time')
                    print('pattern_AQUA')
                    print(pattern_AQUA)
                if '_M02_' in key:
                    pattern_M02 = pattern_from_file_sequence(key, 'time')
                    print('pattern_M02')
                    print(pattern_M02)
                else:
                    continue
