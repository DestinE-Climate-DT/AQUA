reader = Reader(model="ICON", exp="ngc2009",  configdir=configdir, source="atm_2d_ml_R02B09", regrid="r200")
ICON_2009 = reader.retrieve()
ICON_2009_chunk = ICON_2009['tprate'][0:max_time_step,:]
ICON_2009_chunk = ICON_2009_chunk.compute()

ICON_2009_chunk= xarray_attribute_update(ICON_2009_chunk, ICON_2009)
# Fastest histogram
hist_fast_ICON  = diag.hist1d_fast(ICON_2009_chunk,  preprocess = False)

TEST_SIZE = data_size(data_size)