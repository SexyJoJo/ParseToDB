import json
import os
from datetime import datetime
import pymysql
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

    for index, row in df.iterrows():
        tmpList = row.index.tolist()
        for i in range(len(tmpList)):
            # 去掉表头字段多余空格
            tmpList[i] = str.strip(tmpList[i])
            # 除去通道频率前缀的影响：提取出频率的数字组成新的索引
            # if re.match(Consts.CHANNEL_PREFIX, tmpList[i]):
            #     # 获取频率数字字段
            #     tmpList[i] = tmpList[i][-1:-7:-1][::-1]
        row.index = tmpList

        # 根据通道频率名称来映射出所有的通道值 组织成json来存储
        brightness_emperature_43channels = {}
        for channel in mapped_channels:
            brightness_emperature_43channels[channel] = row[channel]
        brightness_emperature_43channels = json.dumps(brightness_emperature_43channels)

        data_info = {
            "id": 'null',
            "lv1_file_id": uuid.uuid3(uuid.NAMESPACE_DNS, full_path + factory),
            "datetime": row[Consts.LV1_TXT_TIME],
            "temperature": row[Consts.LV1_TXT_TEMP],
            "humidity": row[Consts.LV1_TXT_HUMIDITY],
            "pressure": row[Consts.LV1_TXT_PRESSURE],
            "tir": row[Consts.LV1_TXT_TIR],
            "is_rain": row[Consts.LV1_TXT_RAIN],
            "qcisDelete": row[Consts.LV1_TXT_QCFLAG],
            "az": row[Consts.LV1_TXT_AZ],
            "ei": row[Consts.LV1_TXT_EI],
            "qcisDelete_bt": row[Consts.LV1_TXT_QCFlag_BT],
            "brightness_emperature_43channels": brightness_emperature_43channels,
            "isDelete": 0
        }

        try:
            sql = "SELECT datetime FROM  t_lv1_data WHERE datetime = '%s'" \
                  % (data_info["datetime"])
            result = db.execute(sql)
            if not result:
                # 不存在则插入
                sql = "INSERT INTO t_lv1_data(id, lv1_file_id, datetime, temperature, humidity, pressure, tir," \
                      " is_rain, qcisDelete, az, ei, qcisDelete_bt, brightness_emperature_43channels, isDelete) " \
                      "VALUES (%s, '%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, '%s', '%s', %s)" \
                      % (data_info["id"], data_info["lv1_file_id"], data_info["datetime"],
                         data_info["temperature"], data_info["humidity"], data_info["pressure"],
                         data_info["tir"], data_info["is_rain"], data_info["qcisDelete"],
                         data_info["az"], data_info["ei"], data_info["qcisDelete_bt"],
                         data_info["brightness_emperature_43channels"], data_info["isDelete"])
            else:
                # 存在则更新
                sql = "UPDATE t_lv1_data SET temperature=%s, humidity=%s, pressure=%s, tir=%s," \
                      " is_rain=%s, qcisDelete=%s, az=%s, ei=%s, qcisDelete_bt='%s', " \
                      "brightness_emperature_43channels='%s', isDelete=%s WHERE datetime='%s'" \
                      % (data_info["temperature"], data_info["humidity"],
                         data_info["pressure"], data_info["tir"], data_info["is_rain"],
                         data_info["qcisDelete"], data_info["az"], data_info["ei"],
                         data_info["qcisDelete_bt"], data_info["brightness_emperature_43channels"],
                         data_info["isDelete"], data_info["datetime"])
            db.execute(sql)

        except (pd.errors.EmptyDataError, pd.errors.ParserError):  # 出现异常格式或空白df则跳过
            print(f"{filename}文件为空或格式错误")
        cnt += 1
    return data_info


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
            sql = "INSERT INTO t_lv1_file(id, wbfsj_id, obs_time, file_path, file_name, isDelete) " \
                  "VALUES ('%s', %s, '%s', '%s', '%s' ,%s)" % (file_info["id"],
                                                               file_info["wbfsj_id"],
                                                               file_info["obs_time"],
                                                               file_info["file_path"],
                                                               file_info["file_name"],
                                                               file_info["isDelete"])
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

