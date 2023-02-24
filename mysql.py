import pymysql
import pandas as pd


# 用于读取数据表生成sql语句
def create_sql_grammar(path_excel, sheet_name, path_txt):
    """
    该函数用于读取excel表的sheet表生成MySQL语句
    :param path_excel:用于excel来显示数据库的字段信息表路径
    :param sheet_name:excel的具体sheet名
    :param path_txt:写入mysql语句的文件路径
    :return:
    :remark:import pandas as pd
    """
    # path_excel = r"D:\02 ODS基础数据表V2.0.xlsx"
    # sheet_name = 'aliExpress_bill'

    print("==={}===".format("读取配置表"))
    df = pd.read_excel(path_excel, sheet_name=sheet_name, header=1)  # 读取excel表中的数据库模型结果信息
    df_list = df.to_dict('records')  # 将datafrom转为字典

    print("==={}===".format("清空文档表"))
    file = open(path_txt, 'w')

    print("==={}===".format("生成sql建表语句"))
    aa = 'CREATE TABLE `{}` (\n'.format(sheet_name)
    with open(path_txt, 'a+') as f:
        f.write(aa)

    for i in range(len(df_list)):

        if df_list[i]['类型'] == 'datetime':
            aa = '  `{}` datetime DEFAULT NULL COMMENT \'{}\',\n'.format(df_list[i]['字段名'], df_list[i]['注释'])
            with open(path_txt, 'a+') as f:
                f.write(aa)

        elif df_list[i]['字段名'] == 'id':
            aa = '  `{}` int NOT NULL AUTO_INCREMENT COMMENT \'{}\',\n'.format(df_list[i]['字段名'], df_list[i]['注释'])
            with open(path_txt, 'a+') as f:
                f.write(aa)

        elif df_list[i]['类型'] == 'varchar':
            aa = '  `{}` varchar({}) DEFAULT NULL COMMENT \'{}\',\n'.format(df_list[i]['字段名'], df_list[i]['长度'],
                                                                            df_list[i]['注释'])
            with open(path_txt, 'a+') as f:
                f.write(aa)

        elif df_list[i]['类型'] == 'decimal':
            aa = '  `{}` decimal{} DEFAULT NULL COMMENT \'{}\',\n'.format(df_list[i]['字段名'], df_list[i]['长度'],
                                                                          df_list[i]['注释'])
            with open(path_txt, 'a+') as f:
                f.write(aa)
        elif df_list[i]['类型'] == 'int':
            aa = ' `{}` int DEFAULT NULL COMMENT \'{}\',\n'.format(df_list[i]['字段名'], df_list[i]['注释'])
            with open(path_txt, 'a+') as f:
                f.write(aa)
        if df_list[i]['类型'] == 'date':
            aa = '  `{}` datetime DEFAULT NULL COMMENT \'{}\',\n'.format(df_list[i]['字段名'], df_list[i]['注释'])
            with open(path_txt, 'a+') as f:
                f.write(aa)

    aa = '  PRIMARY KEY (`id`)\n) ENGINE=InnoDB AUTO_INCREMENT=49228 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT=\'{}\';'.format(
        sheet_name)
    with open(path_txt, 'a+') as f:
        f.write(aa)
    print("完成sql建表语句")

    # path_excel = r"D:\02 ODS基础数据表V2.0.xlsx"
    # sheet_name = 'Sheet2'
    # path_txt = r"D:\新建文本文档 (3).txt"
    # create_sql_grammar(path_excel, sheet_name, path_txt)


# 查询数据库，生成汇总表
def get_table_data(sql_txt, data_config):
    """
    该函数用于mysql的数据查询
    :param sql_txt: 需要查询的mysql语句
    :param data_config: 需要连接的mysql配置信息
    :return:返回tadaFrom数据
    :remark:import pymysql
    """

    # 连接mysql
    conn = pymysql.connect(host=data_config["数据库ip"], user=data_config["用户名"],
                           passwd=data_config["数据库密码"], db=data_config["数据库名称"],
                           port=int(data_config["端口"]))  # 连接数据库
    df = pd.read_sql(sql_txt, conn)  # 执行mysql语句
    conn.close()  # 关闭数据库连接
    return df
    # data_config = {"数据库ip": "10.200.10.24", "用户名": "rpa", "数据库密码": "", "数据库名称": "rpa",
    #                "端口": "3309"}
    # sql_txt = f"""select * from {table_data["数据库表名"]} where data_time_period = '{table_data["时间周期"]}' order by acquisition_time"""


