import xarray as xr

# 读取 GRIB2 文件
# file_path = r'C:\Users\GX\Desktop\fnl_20230902_18_00.grib2'
file_path2 = r'E:\Jupyter\NCAR\ozone\FNL\2023\fnl_20230901_18_00.grib2'
# ds = xr.open_dataset(file_path, engine='cfgrib', backend_kwargs={'filter_by_keys': {'typeOfLevel': 'surface'}})
ds = xr.open_dataset(file_path2, engine='cfgrib', backend_kwargs={'filter_by_keys': {'typeOfLevel': 'surface'}})


# 查看数据集信息
print(ds)

