"""
这个脚本用于抓取最新更新（当日）的数据文件
"""

import xarray as xr
import os
import datetime
import requests
import warnings
import mysql.connector

"""忽略警告"""
warnings.filterwarnings('ignore')


class NCARDataClawer():
    __year = 2023
    __values = {'email': '12132211@mail.sustech.edu.cn', 'passwd': 'mia444134435', 'action': 'login'}
    __login_url = 'https://rda.ucar.edu/cgi-bin/login'
    __save_dir = f'E:/jupyter/NCAR/ozone/FNL/{__year}/'

    def run(self, typeoflevel, level):
        '''
        运行入口，根据输入的参数筛选需要的数据集
        :param typeoflevel:
        :param level:
        :return:
        '''
        self.__getData()
        self.__writeDatabase(typeoflevel, level)
        print('抓取完成！')

    def __getYesterday(self, delta=1):
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=delta)
        return yesterday

    def __getData(self):
        '''
        下载指定文件
        '''
        # post
        res = requests.post(self.__login_url, data=self.__values)
        if res.status_code != 200:
            print('Bad Authentication')
            print(res.text)
            exit(1)

        # 找到最新文件
        '''注意修改路径头'''
        dspath = 'https://stratus.rda.ucar.edu/ds083.2/'
        y_date = self.__getYesterday(1).strftime('%Y%m%d')
        filelist = [
            f'grib2/{self.__year}/{self.__year}.' + self.__getYesterday(2).strftime('%Y%m%d')[4:6] + '/fnl_' + self.__getYesterday(2).strftime('%Y%m%d') + '_18_00.grib2',
            f'grib2/{self.__year}/{self.__year}.' + y_date[4:6] + '/fnl_' + y_date + '_00_00.grib2',
            f'grib2/{self.__year}/{self.__year}.' + y_date[4:6] + '/fnl_' + y_date + '_06_00.grib2',
            f'grib2/{self.__year}/{self.__year}.' + y_date[4:6] + '/fnl_' + y_date + '_12_00.grib2'
        ]

        # 按文件名依次get
        for file in filelist:
            filename = dspath + file
            outfile = self.__save_dir + os.path.basename(filename)
            print('正在下载', file)
            req = requests.get(filename, cookies=res.cookies, allow_redirects=True)
            open(outfile, 'wb').write(req.content)

    def __writeDatabase(self, typeoflevel, level):
        '''
        读取文件中需要的内容写入mysql
        :param typeoflevel:
        :param level:
        :param year:
        :return:
        '''
        """忽略警告"""
        warnings.filterwarnings('ignore')

        y_date = self.__getYesterday(1).strftime('%Y%m%d')
        # 文件列表
        readfilelist = [
            'fnl_' + self.__getYesterday(2).strftime('%Y%m%d') + '_18_00.grib2',
            'fnl_' + y_date + '_00_00.grib2',
            'fnl_' + y_date + '_06_00.grib2',
            'fnl_' + y_date + '_12_00.grib2'
        ]

        # 文件内容读取
        for file in readfilelist:
            file_path = rf'E:\Jupyter\NCAR\ozone\FNL\{self.__year}\{file}'
            """这里可改，根据传入的参数筛选数据"""
            ds = xr.open_dataset(file_path, engine='cfgrib',backend_kwargs={'filter_by_keys': {'typeOfLevel': f'{typeoflevel}'}})
            print('正在将', file, '中的需求字段写入数据库')
            """table名可改"""
            table_name = file[4:18]
            self.__MySQLController(table_name, ds)

    def __MySQLController(self, table_name, ds):
        '''
        mysql操作
        :param table_name:表名
        :param ds: 数据集
        :return:
        '''
        # 连接到 MySQL 数据库
        cnx = mysql.connector.connect(
            host='192.168.145.131',
            user='root',
            password='123',
            database='NCAR'
        )

        # 创建游标对象
        cursor = cnx.cursor()

        # 建表
        create_table_query = """
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            latitude FLOAT,
            longitude FLOAT,
            value FLOAT
        )
        """.format(table_name=table_name)
        cursor.execute(create_table_query)

        # 提交建表更改
        cnx.commit()

        # 提取数据并插入到数据库中
        """这里可改，从数据集中点出具体要的数据字段塞进数据库，SQL对应修改"""
        for lat, lon, value in zip(ds.latitude.values.flatten(), ds.longitude.values.flatten(),
                                   ds['t'].values.flatten()):
            insert_query = "INSERT INTO `{table_name}`  (latitude, longitude, value) VALUES (%s, %s, %s)".format(table_name=table_name)
            # 将value转换为float类型
            data = (lat, lon, float(value))
            cursor.execute(insert_query, data)

        # 提交更改并关闭连接
        cnx.commit()
        cursor.close()
        cnx.close()


# 实例化并启动
crawler = NCARDataClawer()
crawler.run('surface', 500)
