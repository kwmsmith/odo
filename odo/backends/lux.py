from __future__ import absolute_import, division, print_function

from odo import resource, convert

import pandas as pd
import dask.dataframe as dd
from lux import lux

@resource.register('lux://.+', priority=16)
def resource_lux(uri, **kwargs):
    root_dir = uri[len('lux://'):]
    return lux.lux(root=root_dir, **kwargs)


@convert.register(pd.DataFrame, lux.LuxDataFrame)
def convert_luxdataframe_to_df(ldf, **kwargs):
    return ldf.to_pandas()


@convert.register(dd.DataFrame, lux.LuxDataFrame)
def convert_luxdataframe_to_dask_df(ldf, **kwargs):
    return ldf.to_dask()
