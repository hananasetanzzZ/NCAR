import xarray as xr
import mysql.connector
import datetime

def getYesterday(delta=1):
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=delta)
    return yesterday
y_date = getYesterday(1).strftime('%Y%m%d')



# 读取 GRIB2 文件
file_path = rf'C:\Users\GX\Desktop\fnl_{y_date}_18_00.grib2'
ds = xr.open_dataset(file_path, engine='cfgrib', backend_kwargs={'filter_by_keys': {'typeOfLevel': 'surface'}})
print(ds)

# 连接到 MySQL 数据库
cnx = mysql.connector.connect(
    host='192.168.145.131',
    user='root',
    password='123',
    database='NCAR'
)


# 创建一个游标对象
cursor = cnx.cursor()

# 建表
create_table_query = """
CREATE TABLE IF NOT EXISTS `{table_name}` (
    id INT AUTO_INCREMENT PRIMARY KEY,
    latitude FLOAT,
    longitude FLOAT,
    value FLOAT
)
""".format(table_name=y_date)
cursor.execute(create_table_query)

# 提交更改
cnx.commit()

# 提取数据并插入到数据库中
for lat, lon, value in zip(ds.latitude.values.flatten(), ds.longitude.values.flatten(), ds['t'].values.flatten()):
    insert_query = "INSERT INTO `{table_name}`  (latitude, longitude, value) VALUES (%s, %s, %s)".format(table_name=y_date)
    # 将value转换为float类型
    data = (lat, lon, float(value))
    cursor.execute(insert_query, data)

# 提交更改并关闭连接
cnx.commit()
cursor.close()
cnx.close()