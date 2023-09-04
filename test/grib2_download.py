
import os
import datetime
import requests
import warnings
import xarray as xr


"""忽略警告"""
warnings.filterwarnings('ignore')


def getYesterday(delta=1):
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=delta)
    return yesterday


# 信息初始化
year = 2023
pwd = 'mia444134435'
values = {'email': '12132211@mail.sustech.edu.cn', 'passwd': pwd, 'action': 'login'}
login_url = 'https://rda.ucar.edu/cgi-bin/login'
save_dir = f'E:/jupyter/NCAR/ozone/FNL/{year}/'


# post
res = requests.post(login_url, data=values)
if res.status_code != 200:
    print('Bad Authentication')
    print(res.text)
    exit(1)


# 找到最新文件
dspath = 'https://stratus.rda.ucar.edu/ds083.2/'
y_date = getYesterday(1).strftime('%Y%m%d')
filelist = [f'grib2/{year}/{year}.' + getYesterday(2).strftime('%Y%m%d')[4:6] + '/fnl_' + getYesterday(2).strftime('%Y%m%d') + '_18_00.grib2',
            f'grib2/{year}/{year}.' + y_date[4:6] + '/fnl_' + y_date + '_00_00.grib2',
            f'grib2/{year}/{year}.' + y_date[4:6] + '/fnl_' + y_date + '_06_00.grib2',
            f'grib2/{year}/{year}.' + y_date[4:6] + '/fnl_' + y_date + '_12_00.grib2']

readfilelist = [ 'fnl_' + getYesterday(2).strftime('%Y%m%d') + '_18_00.grib2',
                'fnl_' + y_date + '_00_00.grib2',
                'fnl_' + y_date + '_06_00.grib2',
                'fnl_' + y_date + '_12_00.grib2']


# 按文件名依次get
for file in filelist:
    filename = dspath + file
    outfile = save_dir + os.path.basename(filename)
    print('Downloading', file)
    req = requests.get(filename, cookies=res.cookies, allow_redirects=True)
    open(outfile, 'wb').write(req.content)






# test
# f = 'fnl_' + y_date + '_00_00.grib2'
# print(f)
# file_path = r'E:\Jupyter\NCAR\ozone\FNL\2023\fnl_20230902_00_00.grib2'
# ds = xr.open_dataset(file_path, engine='cfgrib', backend_kwargs={'filter_by_keys': {'typeOfLevel': 'surface'}})
# print(ds)
