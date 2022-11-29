from os.path import join
import s3fs
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe

url_base = 's3://noaa-cdr-total-solar-irradiance-pds/data/daily/'
file_list = []
fs = s3fs.S3FileSystem(anon=True)


def is_nc(x):
    return x.endswith('.nc') and 'preliminary' not in x


def add_s3(x):
    return 's3://' + x


file_list = sorted([add_s3(nc_file) for nc_file in fs.ls(join(url_base)) if is_nc(nc_file)])
pattern = pattern_from_file_sequence(file_list, 'time', nitems_per_file=1)
recipe = HDFReferenceRecipe(pattern, netcdf_storage_options={'anon': True})
