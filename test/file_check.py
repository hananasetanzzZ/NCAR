import cfgrib

file_path = r'E:\Jupyter\NCAR\ozone\FNL\2023\fnl_20230901_18_00.grib2'

# cfgrib库打开文件
with cfgrib.open_file(file_path) as file:
    # 读取第一条消息
    message = next(file)

    # 输出消息内容
    print(message)

# 如果上述代码能够成功运行并输出消息内容，则文件有效
