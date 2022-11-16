import pandas as pd
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe

start_date = "1981-09-01"


def format_function(time):
    """Returns the url to an item for a time based on a dataset specific url pattern.

    NOAA sst optimum interpolation items will be organized along the time dimesnion, so this
    func takes a time argument and the ConcatDim used is "time"

    Args:
        time (int): The numnber of days to use as the time delta that is added to the dataset 
            start date. Represents the date of the item.

    Returns:
        str: The url to the item.
    """
    base = pd.Timestamp(start_date)
    day = base + pd.Timedelta(days=time)
    input_url_pattern = (
        "s3://noaa-cdr-sea-surface-temp-optimum-interpolation-pds/data"
        "/v2.1/avhrr/{day:%Y%m}/oisst-avhrr-v02r01.{day:%Y%m%d}.nc"
    )
    return input_url_pattern.format(day=day)


dates = pd.date_range(start_date, "2022-11-08", freq="D")
pattern = FilePattern(format_function, ConcatDim("time", range(len(dates)), nitems_per_file=1))

# todos based on args https://pangeo-forge.readthedocs.io/en/latest/pangeo_forge_recipes/api_reference.html?highlight=kerchunk#pangeo_forge_recipes.recipes.HDFReferenceRecipe
# define storageconfig
# define output json and intake fnames if desired different than "reference"
# do we want to set anon = True during the full recipe run?
# not sure what goes into optional target options dict
# ditto inline_threshold
# ditto output_storage_options
# all data vars look like they are not identical. not filling in identical_dims
# not sure what coo_map does, ignoring. maybe this only applies if we were naming the concat dim somethign other than time and what the hdfs internally use?
# assuming postprocess and preprocess not needed

recipe = HDFReferenceRecipe(
    pattern,
    netcdf_storage_options={"anon": True}
)

recipe_pruned = recipe.copy_pruned()
run_function = recipe_pruned.to_function()
run_function()