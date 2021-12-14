"""解析新格式文件，使用df.to_sql入库"""
import json
import os
import sys
from datetime import datetime
import pymysql
from sqlalchemy import create_engine
import Consts
import pandas as pd
import re
from qc_from_db_nochunk import Log

cnt = 0


class MySQL:
    def __init__(self):
        self.conn = pymysql.connect(host='localhost',
                                    user='root',
                                    password='123',
                                    database='microwave')
        self.cur = self.conn.cursor()
        self.data_conn = create_engine('mysql+pymysql://root:123@localhost/microwave?charset=utf8')

    def __del__(self):
        try:
            self.cur.close()
            self.conn.close()
            print('DB Closed!')
        except AttributeError:
            pass

    def execute(self, sql):
        self.cur.execute(sql)
        self.conn.commit()
        return self.cur.fetchone()


def get_mapped_channels(filename, db, data):
    """
    TXT/txt格式的lv1数据文件的解析
        full_path : lv1文件全路径
        file_id : 记录主键
        return: 完整的lv1数据结果集合
    """

    def get_station_id():
        fields = filename.split('_')
        return fields[3]

    sql = f"SELECT channels_map FROM t_device_info WHERE station_id={get_station_id()}"
    ch_map = db.execute(sql)

    channels = []
    channels_withoutCh = []
    for column in data.columns:
        m = re.search(r"\d+\.\d+$", column)
        if m:
            channels.append(column)
            channels_withoutCh.append('Ch ' + m.group())
    return eval(ch_map[0]), channels, channels_withoutCh


# def lv1file_parse(field, full_path, filename, dev_id):
#     file_info = {
#         'id': uuid.uuid3(uuid.NAMESPACE_DNS, str(dev_id) + field[4][:8]),
#         'wbfsj_id': int(field[3]),
#         'obs_time': field[4],
#         'file_path': full_path,
#         'file_name': filename,
#         'isDelete': 0
#     }
#     return file_info


def lv1_txt_parse(full_path, db, filename, parse_logger):
    # txt文件读取
    df = pd.read_csv(full_path, header=2, index_col=0, engine="python", encoding="gbk", dtype={'QCFlag_BT': str})
    df = df.dropna(axis=1, how="all")

    # 替换统一表头
    tmpList = list(df.columns)
    tmpList[0:len(Consts.LV1_TXT_UNIFIED_HEADER
                  )] = Consts.LV1_TXT_UNIFIED_HEADER
    df[Consts.LV1_TXT_TIME] = pd.to_datetime(df[Consts.LV1_TXT_TIME])
    df.drop_duplicates(subset=[Consts.LV1_TXT_TIME])    # 删除重复时间的行

    channel_map, mapped_channels, mapped_channels_withoutCh = get_mapped_channels(filename, db, df)

    df["id"] = 0
    df["lv1_file_name"] = filename
    df["isDelete"] = 0
    df["temp_is_rain"] = None
    df["is_qced"] = 0
    df["my_flag"] = None
    df["station_id"] = filename.split('_')[3]
    for i in range(len(mapped_channels)):
        df = df.rename(columns={mapped_channels[i]: mapped_channels_withoutCh[i]})
    df["brightness_temperature_43channels"] = df[mapped_channels_withoutCh].apply(lambda x: json.dumps(dict(x)), axis=1)
    df = df.rename(columns={"DateTime": "datetime", "SurTem(℃)": "temperature", "SurHum(%)": "humidity",
                            "SurPre(hPa)": "pressure", "Tir(℃)": "tir", "Rain": "is_rain", "Az(deg)": "az",
                            "El(deg)": "ei"})
    df = df.drop(columns=mapped_channels_withoutCh)
    df = df[
        ["id", "station_id", "lv1_file_name", "datetime", "temperature", "humidity", "pressure", "tir", "is_rain",
         "QCFlag", "az", "ei", "QCFlag_BT", "brightness_temperature_43channels", "isDelete", "temp_is_rain",
         "is_qced", "my_flag"]]
    parse_logger.logger.info(f"{filename}解析完毕,数据条数：{df.shape[0]}")

    parse_logger.logger.info("正在存入数据库...")
    df.to_sql('t_lv1_data_temp', con=db.data_conn, if_exists='replace', index=False)
    with db.data_conn.begin() as cn:
        sql = """INSERT INTO t_lv1_data (id, station_id, lv1_file_name, datetime, temperature, humidity, pressure, tir, 
                 is_rain, QCFlag, az, ei, QCFlag_BT, brightness_temperature_43channels, isDelete, temp_is_rain, 
                 is_qced, my_flag)
                 SELECT * FROM t_lv1_data_temp t
                 ON DUPLICATE KEY UPDATE temperature=t.temperature, humidity=t.humidity, pressure=t.pressure, 
                 tir=t.tir, is_rain=t.is_rain, QCFlag=t.QCFlag, az=t.az, ei=t.ei, QCFlag_BT=t.QCFlag_BT,
                 brightness_temperature_43channels=t.brightness_temperature_43channels, isDelete=t.isDelete,
                 station_id=t.station_id, is_qced=t.is_qced"""
        cn.execute(sql)
    parse_logger.logger.info("入库完毕")

def main():
    parse_logger = Log("parse_logger")
    try:
        db = MySQL()
    except pymysql.err.OperationalError:
        parse_logger.logger.error("数据库未连接")
        sys.exit()

    with open("config/parse/new_config.json", 'r', encoding='gbk') as f:
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

            # 解析文件中的数据
            parse_logger.logger.info(f"正在解析{file}...")
            try:
                lv1_txt_parse(fullpath, db, filename, parse_logger)
            except Exception:
                parse_logger.logger.error("解析入库失败")


if __name__ == '__main__':
    start = datetime.now()
    main()
    end = datetime.now()

    print(f"开始时间：{start}")
    print(f"结束时间：{end}")
    print(f"运行时间:{end - start}")
    print(cnt)
