# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy
import re
import json
import numpy as np

class Mysql:
    def __init__(self):
        self.conn = sqlalchemy.create_engine('mysql+pymysql://root:123@localhost/microwave?charset=utf8')

    def get_station_id(self):
        """从数据库中读取各个站台号"""
        sql = "SELECT station_id FROM t_device_info"
        station_ids = self.conn.execute(sql).fetchall()
        station_ids = [x[0] for x in station_ids]
        return station_ids

    def get_data(self, station_id):
        """根据设备号查询未质控的数据，分块读取需要用于质控的数据"""
        sql = f"SELECT station_id, datetime, is_rain, brightness_temperature_43channels FROM t_lv1_data " \
              f"WHERE station_id={station_id} AND my_flag IS NULL " \
              f"ORDER BY datetime ASC"
        dataset = pd.read_sql(sql, self.conn, chunksize=30)
        print(f"db执行：{sql}")
        return dataset

    def get_rollback_data(self, station_id):
        sql = f"SELECT station_id, datetime, is_rain, brightness_temperature_43channels FROM t_lv1_data " \
              f"WHERE station_id={station_id} AND my_flag IS NOT NULL " \
              f"ORDER BY datetime DESC limit 5"
        rollback_data = pd.read_sql(sql, self.conn)
        return rollback_data

    def get_channel_map(self, station_id):
        sql = f"SELECT channels_map FROM t_device_info WHERE station_id={station_id}"
        channel_map = self.conn.execute(sql)
        return channel_map

    def update_flag(self, df):
        """更新数据库中的质控码"""
        df.to_sql('temp_flag', con=self.conn, if_exists='replace', index=False)
        with self.conn.begin() as cn:
            sql = """INSERT INTO t_lv1_data (my_flag)
                     SELECT my_flag FROM temp_flag t
                     WHERE station_id = t.station_id AND datetime = t.datetime"""
            cn.execute(sql)

def quality_control():
    """质控"""
    def spread_df(data):
        """将brightness_temperature_43channels列展开为dataframe，与原表拼接"""
        temp = json.dumps([json.loads(line) for line in data["brightness_temperature_43channels"]])
        temp = pd.read_json(temp)
        del data["brightness_temperature_43channels"]
        data = pd.concat([data, temp], axis=1)
        return data

    def get_channels(columns):
        """获得通道列表"""
        channels = []
        for column in columns:
            m = re.search(r"\d+\.\d+$", column)
            if m:
                channels.append(column)
        return channels

    def modify_flag(flag, index, val):
        """将flag中的index位替换为其他数值val"""
        s = list(flag)
        s[index] = val
        flag = ''.join(s)
        return flag

    def check_n1():
        """逻辑检查 : 亮温测量范围应为水汽通道7~320K，氧气通道30~320K。通道频率小于40为水汽通道，大于40为氧气通道"""
        def classify_channels(channels):
            """将通道分类为水汽通道和氧气通道"""
            vapor_channels = []
            oxygen_channels = []
            for channel in channels:
                m = re.search(r"\d+\.\d+$", channel)
                if float(m.group()) < 40:
                    vapor_channels.append(channel)
                elif float(m.group()) > 40:
                    oxygen_channels.append(channel)
            return vapor_channels, oxygen_channels

        vapor_channel, oxygen_channel = classify_channels(ch_data.columns)
        vapor_data = ch_data[vapor_channel]
        oxygen_data = ch_data[oxygen_channel]
        vapor_condition = ((vapor_data > 320) | (vapor_data < 7)) | vapor_data.isnull()
        oxygen_condition = ((oxygen_data > 320) | (oxygen_data < 30)) | oxygen_data.isnull()
        # 获得需要更改QCFlag_BT的索引列表
        vapor_modi_index = vapor_data[vapor_condition].dropna(how='all').index
        oxygen_modi_index = oxygen_data[oxygen_condition].dropna(how='all').index
        # 修改质控码
        merged_data.loc[vapor_modi_index, 'my_flag'] = \
            merged_data.loc[vapor_modi_index, 'my_flag'].apply(modify_flag, index=0, val='2')
        merged_data.loc[oxygen_modi_index, 'my_flag'] = \
            merged_data.loc[oxygen_modi_index, 'my_flag'].apply(modify_flag, index=0, val='2')

    def check_n2():
        """ 最小变率检查 : 若任意通道亮温连续3个数值保持不变（不考虑时间间隔），则该时次质量标识n2标记为1;（回溯当前文件前一天最新的2条记录）
        若任意通道亮温连续5个数值保持不变（不考虑时间间隔），则该时次质量标识n2标记为2;（回溯当前文件前一天最新的4条记录）"""
        for column in ch_data.columns:
            values = ch_data[column]  # 每列的数值列表
            for index in range(len(rollback_df), len(ch_data)):
                if index == 0:
                    continue
                # 检查n2是否经过前面通道的更改已经变为2，如果已经为2则检查下一行
                if merged_data.loc[index, 'my_flag'][1] == '2':
                    continue
                # 统计重复个数
                cnt = 1
                i = index
                try:
                    while abs(values[i] - values[i-1]) < 0.001:  # 当前值较前一个值不变
                        cnt += 1
                        i -= 1  # 继续向前比较
                        if cnt == 5:
                            break
                except KeyError:
                    continue
                # 更改QCFlag_BT中的n2
                if cnt == 5:
                    merged_data.loc[index, 'my_flag'] = \
                        modify_flag(merged_data.loc[index, 'my_flag'], 1, '2')
                elif 3 <= cnt < 5:
                    merged_data.loc[index, 'my_flag'] = \
                        modify_flag(merged_data.loc[index, 'my_flag'], 1, '1')

            # 由于回溯时无法完全质控每块前4条数据，因此记录当前数据块(chunk)的最后四条质控码覆盖到下一个块的前四条
            # 防止下一个数据块前四条质控码丢失
            last_4_flag = merged_data.loc[-4:, 'my_flag']

    def check_n3():
        # 设备自带降水标记判别：当降水标识为1，则该时次质量标识n3标记为2
        merged_data.loc[merged_data['is_rain'] == 1, 'my_flag'] = \
            merged_data.loc[merged_data['is_rain'] == 1, 'my_flag'].apply(modify_flag, index=2, val='2')

        # 若第1通道(22GHz左右）亮温＞169K，则该时次质量标识n3标记为1，降水标识（不改原文件，仅指中间过程文件）为1

    # 入口
    db = Mysql()
    station_ids = db.get_station_id()

    for station_id in station_ids:
        channel_map = db.get_channel_map(station_id)    # 数据库中的映射列表
        dataset = db.get_data(station_id)

        rollback_df = db.get_rollback_data(station_id)
        rollback_df = spread_df(rollback_df)
        for df in dataset:
            # 拼接df和用于回溯的数据
            df = spread_df(df)
            merged_data = pd.concat([rollback_df, df], axis=0, ignore_index=True)
            merged_data['my_flag'] = '00000'
            # 提取通道数据
            ch_data = merged_data[get_channels(merged_data.columns)]

            # 开始质控
            check_n1()
            check_n2()
            check_n3()
            print(merged_data)

            # db.update_flag(merged_data[["station_id", "datetime", "my_flag"]])
            rollback_df = df.iloc[-4:, :]


def main():
    pd.set_option('display.max_columns', 100)
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.width', 500)
    quality_control()


if __name__ == '__main__':
    main()
