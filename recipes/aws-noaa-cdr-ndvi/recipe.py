import s3fs

from pangeo_forge_recipes.patterns import pattern_from_file_sequence

# from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe

url_base = 's3://noaa-cdr-ndvi-pds/data/'

# years = range(1981, 2022)
years = range(1981, 1982)
file_list = []

fs = s3fs.S3FileSystem(anon=True)

for year in years:
    file_list += sorted(
        filter(lambda x: x.endswith('.nc'), fs.ls(url_base + str(year), detail=False))
    )

# file pattern identification a work-in-progress

pattern = pattern_from_file_sequence(file_list, 'time', nitems_per_file=1)

for key in pattern:
    break
key
print(pattern[key])

# recipe = HDFReferenceRecipe(pattern, netcdf_storage_options={'anon': True})
