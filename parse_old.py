import pandas as pd
import pymysql
import json
import os
import uuid
import re
import numpy as np
from datetime import datetime


class MySQL:
    def __init__(self):
        self.conn = pymysql.connect(host='localhost',
                                    user='root',
                                    password='123',
                                    database='microwavw_localization_reconstruct')
        self.cur = self.conn.cursor()

    def __del__(self):
        self.cur.close()
        self.conn.close()
        print('DB Closed!')

    def execute(self, sql):
        self.cur.execute(sql)
        self.conn.commit()
        print(f"执行 {sql}")
        return self.cur.fetchone()


class OldParser:
    """旧格式csv解析器"""
    def __init__(self):
        # 读取配置
        with open("old_config.json", "r", encoding='utf-8') as f:
            config = json.load(f)
            self.dir_path = config["dir_path"]
            self.ch_index = (np.array(config["ch_index"]) + 7).tolist()  # 通道索引

        # 初始化用于保存数据的容器
        self.file_info = {}
        self.data_info = {}

    def get_df(self):
        index = [x for x in range(0, 7)] + self.ch_index  # 需要用到的列索引
        return pd.read_csv(self.filepath).iloc[:, index]

    def parse_file_info(self):
        """解析单个文件的文件信息： [id, wbfsj_id, obs_time, file_path, file_name, isDelete]"""
        self.file_info["id"] = str(uuid.uuid4())
        self.file_info["wbfsj_id"] = "null"  # 让该字段自增
        self.file_info["obs_time"] = re.search(r'\d{4}-\d{2}-\d{2}', self.filename).group()
        self.file_info["file_path"] = self.filepath.replace("\\", "\\\\")
        self.file_info["file_name"] = self.filename
        self.file_info["isDelete"] = 0

    def parse_data_info(self, line, column_names):
        """
        解析单个文件的数据信息：
        [id, lv1_file_id, datetime, temperature, humidity, pressure, tir, is_rain, qcisDelete, az, ei, qcisDelete_bt,
        brightness_emperature_43channels, isDelete]
        """

        def get_chdata():
            """归纳通道数据， 保存为json格式"""
            chdata = {}
            for index in range(7, 7 + len(self.ch_index)):
                chdata[column_names[index]] = line[index]
            data = json.dumps(chdata)
            return data

        self.data_info["id"] = "null"
        self.data_info["lv1_file_id"] = self.file_info["id"]
        self.data_info["datetime"] = line[1]
        self.data_info["temperature"] = line[2]
        self.data_info["humidity"] = line[3]
        self.data_info["pressure"] = line[4]
        self.data_info["tir"] = line[5]
        self.data_info["is_rain"] = line[6]
        self.data_info["qcisDelete"] = "null"
        self.data_info["az"] = "null"
        self.data_info["ei"] = "null"
        self.data_info["qcisDelete_bt"] = "null"
        self.data_info["brightness_emperature_43channels"] = get_chdata()
        self.data_info["isDelete"] = 0


class NewParser:
    """新格式txt解析器"""

def main():
    db = MySQL()
    parser = OldParser()
    cnt = 0

    # 遍历每个文件对其解析
    for root, _, files in os.walk(parser.dir_path):
        for file in files:
            parser.filename = file
            parser.filepath = os.path.join(root, file)  # parser当前处理的文件路径

            # 解析文件信息并入库
            parser.parse_file_info()
            sql = "INSERT INTO t_lv1_file(id, wbfsj_id, obs_time, file_path, file_name, isDelete) " \
                  "VALUES ('%s', %s, '%s', '%s', '%s' ,%s)" % (parser.file_info["id"],
                                                               parser.file_info["wbfsj_id"],
                                                               parser.file_info["obs_time"],
                                                               parser.file_info["file_path"],
                                                               parser.file_info["file_name"],
                                                               parser.file_info["isDelete"])
            db.execute(sql)

            # 解析数据信息并入库
            try:
                parser.df = parser.get_df()  # parser当前处理的df
                # 遍历当前df中的每一行
                for index in parser.df.index:
                    # 解析数据
                    parser.parse_data_info(parser.df.loc[index].values, parser.df.columns)
                    # 若存在则更新，不存在则插入
                    # 查询是否存在
                    sql = "SELECT datetime FROM  t_lv1_data WHERE datetime = '%s'" \
                          % (parser.data_info["datetime"])
                    result = db.execute(sql)
                    cnt += 1
                    print(result, cnt)
                    if not result:
                        # 不存在则插入
                        sql = "INSERT INTO t_lv1_data(id, lv1_file_id, datetime, temperature, humidity, pressure, tir," \
                              " is_rain, qcisDelete, az, ei, qcisDelete_bt, brightness_emperature_43channels, isDelete) " \
                              "VALUES (%s, '%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, '%s', %s)" \
                              % (parser.data_info["id"], parser.data_info["lv1_file_id"], parser.data_info["datetime"],
                                 parser.data_info["temperature"], parser.data_info["humidity"], parser.data_info["pressure"],
                                 parser.data_info["tir"], parser.data_info["is_rain"], parser.data_info["qcisDelete"],
                                 parser.data_info["az"], parser.data_info["ei"], parser.data_info["qcisDelete_bt"],
                                 parser.data_info["brightness_emperature_43channels"], parser.data_info["isDelete"])
                    else:
                        # 存在则更新
                        sql = "UPDATE t_lv1_data SET temperature=%s, humidity=%s, pressure=%s, tir=%s," \
                              " is_rain=%s, qcisDelete=%s, az=%s, ei=%s, qcisDelete_bt=%s, " \
                              "brightness_emperature_43channels='%s', isDelete=%s WHERE datetime='%s'"\
                              % (parser.data_info["temperature"], parser.data_info["humidity"],
                                 parser.data_info["pressure"], parser.data_info["tir"], parser.data_info["is_rain"],
                                 parser.data_info["qcisDelete"], parser.data_info["az"], parser.data_info["ei"],
                                 parser.data_info["qcisDelete_bt"], parser.data_info["brightness_emperature_43channels"],
                                 parser.data_info["isDelete"], parser.data_info["datetime"])
                    db.execute(sql)

            except (pd.errors.EmptyDataError, pd.errors.ParserError):  # 出现异常格式或空白df则跳过
                print(f"{parser.filename}文件为空或格式错误")


if __name__ == '__main__':
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"开始时间：{start}")
    print(f"结束时间：{end}")
    print(f"运行时间:{end - start}")
    # except Exception as e:
    #     print(e)
