from os.path import join
import pandas as pd
import s3fs
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe
from dask.diagnostics import ProgressBar

url_base = "s3://noaa-cdr-precip-cmorph-pds/data/daily/0.25deg/"
file_list = []
fs = s3fs.S3FileSystem(anon=True)
dates = pd.date_range("1998", "2022", freq="M")


def is_nc(x):
    return x.endswith(".nc") and "preliminary" not in x


def add_s3(x):
    return "s3://" + x


file_list = []
for d in dates:
    date_path = f"{str(d.year)}/{str(d.month).zfill(2)}/"
    file_list += [add_s3(x) for x in fs.ls(join(url_base, date_path)) if is_nc(x)]
file_list = sorted(file_list)


pattern = pattern_from_file_sequence(file_list, "time", nitems_per_file=1)
recipe = HDFReferenceRecipe(pattern, netcdf_storage_options={"anon": True})
