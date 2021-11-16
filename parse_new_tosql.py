"""解析新格式文件，使用df.to_sql入库"""
import json
import os
from datetime import datetime
import pymysql
from sqlalchemy import create_engine
import Consts
import pandas as pd
import uuid

cnt = 0


class MySQL:
    def __init__(self):
        self.conn = pymysql.connect(host='localhost',
                                    user='root',
                                    password='123',
                                    database='microwavw_newdata')
        self.cur = self.conn.cursor()
        self.data_conn = create_engine('mysql+pymysql://root:123@localhost/microwavw_newdata?charset=utf8')

    def __del__(self):
        self.cur.close()
        self.conn.close()
        print('DB Closed!')

    def execute(self, sql):
        self.cur.execute(sql)
        self.conn.commit()
        print(f"执行 {sql}")
        return self.cur.fetchone()


def get_mapped_channels(filename):
    """
    TXT/txt格式的lv1数据文件的解析
        full_path : lv1文件全路径
        file_id : 记录主键
        return: 完整的lv1数据结果集合
    """

    def get_factory():
        fields = filename.split('_')
        return fields[-3]

    mapped_channels = []
    with open("channel_map.json", "r", encoding='utf-8') as f:
        channel_info = json.load(f)
        factory = get_factory()

        all_channels = channel_info["all_channels"]
        channel_number = channel_info[factory]
        for index in channel_number:
            mapped_channels.append(all_channels[index])
    return mapped_channels


def lv1file_parse(field, full_path, filename):
    file_info = {
        'id': uuid.uuid3(uuid.NAMESPACE_DNS, full_path + field[-3]),
        'wbfsj_id': int(field[3]),
        'obs_time': field[4],
        'file_path': full_path,
        'file_name': filename,
        'isDelete': 0
    }
    return file_info


def Lv1TXTParse(full_path, mapped_channels, factory, db, filename):
    global cnt
    # txt文件读取
    df = pd.read_csv(full_path, header=2, index_col=0, engine="python", encoding="gbk")
    print("-------TXTfile: ", full_path)
    print("行数: ", df.shape[0])
    df = df.dropna(axis=1, how="all")
    # 替换统一表头
    tmpList = list(df.columns)
    # print(df)
    tmpList[0:len(Consts.LV1_TXT_UNIFIED_HEADER
                  )] = Consts.LV1_TXT_UNIFIED_HEADER

    # print("通道映射关系：", channel_number)
    # print("通道列头：", frequencyHeader)

    df[Consts.LV1_TXT_TIME] = pd.to_datetime(df[Consts.LV1_TXT_TIME])
    # 删除重复时间的行
    df.drop_duplicates(subset=[Consts.LV1_TXT_TIME])

    df["id"] = 0
    df["lv1_file_id"] = uuid.uuid3(uuid.NAMESPACE_DNS, full_path+factory)
    df["isDelete"] = 0
    df["brightness_emperature_43channels"] = df[mapped_channels].apply(lambda x: json.dumps(dict(x)), axis=1)
    df = df.rename(columns={"DateTime": "datetime", "SurTem(℃)": "temperature", "SurHum(%)": "humidity",
                            "SurPre(hPa)": "pressure", "Tir(℃)": "tir", "Rain": "is_rain", "QCFlag": "qcisDelete",
                            "Az(deg)": "az", "El(deg)": "ei", "QCFlag_BT": "qcisDelete_bt",
                            })
    df = df.drop(columns=mapped_channels)
    df = df[
        ["id", "lv1_file_id", "datetime", "temperature", "humidity", "pressure", "tir", "is_rain", "qcisDelete", "az",
         "ei", "qcisDelete_bt", "brightness_emperature_43channels", "isDelete"]]
    # df.to_sql('t_lv1_data', con=db.data_conn, if_exists='append', index=False)
    print("执行to_sql")
    df.to_sql('t_lv1_data_temp', con=db.data_conn, if_exists='replace', index=False)
    with db.data_conn.begin() as cn:
        sql = """INSERT INTO t_lv1_data (id, lv1_file_id, datetime, temperature, humidity, pressure, tir, is_rain, 
                 qcisDelete, az, ei, qcisDelete_bt, brightness_emperature_43channels, isDelete)
                 SELECT * FROM t_lv1_data_temp t
                 ON DUPLICATE KEY UPDATE temperature=t.temperature, humidity=t.humidity, pressure=t.pressure, 
                 tir=t.tir, is_rain=t.is_rain, qcisDelete=t.qcisDelete, az=t.az, ei=t.ei, qcisDelete_bt=t.qcisDelete_bt,
                 brightness_emperature_43channels=t.brightness_emperature_43channels, isDelete=t.isDelete"""
        cn.execute(sql)
        print(f"执行{sql}")

        # sql = """UPDATE t_lv1_data f, t_lv1_data_temp t
        #          SET f.temperature=t.temperature, f.humidity=t.humidity, f.pressure=t.pressure, f.tir=t.tir, f.is_rain=t.is_rain, f.qcisDelete=t.qcisDelete, f.az=t.az, f.ei=t.ei, f.qcisDelete_bt=t.qcisDelete_bt, f.brightness_emperature_43channels=t.brightness_emperature_43channels, f.isDelete=t.isDelete
        #          WHERE EXISTS
        #                 (SELECT 1 FROM t_lv1_data f
        #                  WHERE t.lv1_file_id = f.lv1_file_id
        #                  AND t.datetime = f.datetime)"""
        # cn.execute(sql)
        # print(f"执行{sql}")


def main():
    db = MySQL()
    with open("new_config.json", 'r', encoding='utf-8') as f:
        dir_path = json.load(f)["dir_path"]

    for root, _, files in os.walk(dir_path):
        # 解析每个文件
        for file in files:
            filename = file
            fullpath = os.path.join(root, file)

            # 如果文件名包含P或者M跳过
            field = filename.split('_')
            try:
                if field[5] == 'P' or field[-1][0] == 'M' or field[-1][2:] != 'txt' or field[-2] in ['CAL', 'STA']:
                    continue
            except IndexError:
                continue

            # 解析文件信息
            file_info = lv1file_parse(field, fullpath, filename)
            sql = "SELECT id FROM  t_lv1_file WHERE id = '%s'" % (file_info["id"])
            result = db.execute(sql)
            if not result:
                sql = "INSERT INTO t_lv1_file(id, wbfsj_id, obs_time, file_path, file_name, isDelete) " \
                      "VALUES ('%s', %s, '%s', '%s', '%s' ,%s)" % (file_info["id"],
                                                                   file_info["wbfsj_id"],
                                                                   file_info["obs_time"],
                                                                   file_info["file_path"],
                                                                   file_info["file_name"],
                                                                   file_info["isDelete"])
            else:
                sql = "UPDATE t_lv1_file SET wbfsj_id =%s, obs_time='%s', file_path='%s'," \
                      "file_name='%s', isDelete=%s WHERE id ='%s'" % \
                      (file_info["wbfsj_id"], file_info["obs_time"], file_info["file_path"],
                       file_info["file_name"], file_info["isDelete"], file_info["id"])
            db.execute(sql)

            # 解析文件中的数据
            mapped_channels = get_mapped_channels(filename)
            Lv1TXTParse(fullpath, mapped_channels, field[-3], db, filename)


if __name__ == '__main__':
    start = datetime.now()
    main()
    end = datetime.now()

    print(f"开始时间：{start}")
    print(f"结束时间：{end}")
    print(f"运行时间:{end - start}")
    print(cnt)
