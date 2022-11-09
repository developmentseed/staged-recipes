import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

start_date = "1981-09-01"


def format_function(time):
    base = pd.Timestamp(start_date)
    day = base + pd.Timedelta(days=time)
    input_url_pattern = (
        "s3://noaa-cdr-sea-surface-temp-optimum-interpolation-pds/data"
        "/v2.1/avhrr/{day:%Y%m}/oisst-avhrr-v02r01.{day:%Y%m%d}.nc"
    )

    return input_url_pattern.format(day=day)


dates = pd.date_range(start_date, "2022-11-08", freq="D")
pattern = FilePattern(format_function, ConcatDim("time", range(len(dates)), 1))
print(pattern)
recipe = XarrayZarrRecipe(pattern, inputs_per_chunk=20, cache_inputs=True)
print(recipe)