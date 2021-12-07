# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy
import re
import json
import numpy as np

global rain_index


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
        sql = f"SELECT station_id, datetime, is_rain, brightness_temperature_43channels, my_flag FROM t_lv1_data " \
              f"WHERE station_id={station_id} AND my_flag IS NULL " \
              f"ORDER BY datetime ASC"
        dataset = pd.read_sql(sql, self.conn, chunksize=300)
        print(f"db执行：{sql}")
        return dataset

    def get_rollback_data(self, station_id):
        """查询最后3个小时质控过的数据用于回溯"""
        sql = f"SELECT datetime FROM t_lv1_data " \
              f"WHERE station_id={station_id} AND my_flag IS NOT NULL " \
              f"ORDER BY datetime DESC limit 1"
        result = self.conn.execute(sql).fetchone()
        if result:
            latest_time = result[0]
            sql = f"SELECT station_id, datetime, brightness_temperature_43channels, temp_is_rain, my_flag " \
                  f"FROM t_lv1_data " \
                  f"WHERE station_id={station_id} AND my_flag IS NOT NULL " \
                  f"AND datetime between '{latest_time-timedelta(hours=3)}' and '{latest_time}' " \
                  f"ORDER BY datetime"
            rollback_data = pd.read_sql(sql, self.conn)
            rollback_data = rollback_data.rename(columns={"temp_is_rain": "is_rain"})
        else:
            rollback_data = pd.DataFrame()
        return rollback_data

    def get_channel_map(self, station_id):
        sql = f"SELECT channels_map FROM t_device_info WHERE station_id={station_id}"
        channel_map = self.conn.execute(sql)
        return channel_map

    def update_flag(self, df):
        """更新数据库中的降水标识和质控码"""
        for index, row in df.iterrows():
            sql = f"""UPDATE t_lv1_data SET temp_is_rain = {row["is_rain"]}, my_flag='{row["my_flag"]}'
                      WHERE station_id='{row["station_id"]}' AND datetime='{row["datetime"]}'"""
            self.conn.execute(sql)
            print(f"执行{sql}")


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

    def get_config():
        with open("config/quality_control/qc_db_config.json", 'r') as f:
            config = json.load(f)
        return config

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
                    while abs(values[i] - values[i - 1]) < 0.001:  # 当前值较前一个值不变
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

    def check_n3():
        """降水检查"""

        def check_n3_1():
            """设备自带降水标记判别：当降水标识为1，则该时次质量标识n3标记为2"""
            merged_data.loc[merged_data['is_rain'] == 1, 'my_flag'] = \
                merged_data.loc[merged_data['is_rain'] == 1, 'my_flag'].apply(modify_flag, index=2, val='2')

        def check_n3_2():
            """若第1通道(22GHz左右）亮温＞169K，则该时次质量标识n3标记为1，降水标识（不改原文件，仅指中间过程文件）为1"""
            ch1 = ch_data.iloc[:, 0]
            result = (ch1 > 169)
            merged_data.loc[result, 'is_rain'] = 1
            result = (ch1 > 169) & (merged_data.loc[:, 'my_flag'].apply(lambda flag: flag[2]) == '0')  # 若n3已经为2，则无需更改
            merged_data.loc[result, 'my_flag'] = \
                merged_data.loc[result, 'my_flag'].apply(modify_flag, index=2, val='1')

        def check_n3_3():
            """当降雨标识为0，且前一时刻降雨标识为1时，若各通道亮温观测值较前一个时次下降，则将该时次n3标记为1，降水标识为1；"""
            for index in range(len(rollback_df) + 1, len(merged_data)):
                # 若n3已经为2或者1，则判断下一行
                if merged_data.loc[index, 'my_flag'][2] != '0':
                    continue

                # 中间出现时间空隙（大于2min），跳过
                time_sep = merged_data.loc[index, "datetime"] - merged_data.loc[index - 1, "datetime"]
                if time_sep > timedelta(minutes=3):
                    continue

                if (merged_data.loc[index, 'is_rain'] == 0) & (merged_data.loc[index - 1, 'is_rain'] == 1):
                    result = (ch_data.loc[index, :] < ch_data.loc[index - 1, :])
                    if False in result.values:  # 没有全部下降
                        continue
                    else:
                        merged_data.loc[index, 'is_rain'] = 1
                        merged_data.loc[index, 'my_flag'] = modify_flag(merged_data.loc[index, 'my_flag'], 2, '1')

        def check_n3_4():
            """当中间文件的降水标识为1，计算与最近一次标识为1的降雨的观测时间差，若小于3小时，则将两次降雨标识之间的数据n3标识为1"""
            if len(merged_data[merged_data["is_rain"] == 1]) in [0, 1]:
                pass
            else:
                rain_data = merged_data[merged_data["is_rain"] == 1]
                for i in range(len(rain_data) - 1):
                    if merged_data.loc[rain_data.index[i], "datetime"] - \
                            merged_data.loc[rain_data.index[i + 1], "datetime"] < timedelta(minutes=360):
                        merged_data.loc[rain_data.index[i]: rain_data.index[i + 1], "my_flag"] = \
                            merged_data.loc[rain_data.index[i]: rain_data.index[i + 1], "my_flag"].apply(modify_flag, index=2, val='1')

        check_n3_1()
        check_n3_2()
        check_n3_3()
        check_n3_4()

    def check_n4():
        """K通道观测亮温与前一个亮温值差值的绝对值≤2K，则通过一致性检验，质量标识n4标记为0
           若差值绝对值>2K且≤4K，质量标识n4标记为1；若差值绝对值>4K，则质量标识n4标记为2。"""

        def get_k_channels():
            k_channels = []
            k_range = get_config()["k_range"]
            for column in ch_data.columns:
                if k_range[0] <= float(column[2:]) <= k_range[1]:
                    k_channels.append(column)
            return k_channels

        k_ch = get_k_channels()
        k_ch_data = ch_data[k_ch]

        for index in range(len(rollback_df) + 1, len(merged_data.index)):
            # 中间出现时间空隙（大于2min），跳过
            time_sep = merged_data.loc[index, "datetime"] - merged_data.loc[index - 1, "datetime"]
            if time_sep > timedelta(minutes=3):
                continue

            result1 = (abs(k_ch_data.loc[index, :] - k_ch_data.loc[index - 1, :]) <= 4) & \
                      (abs(k_ch_data.loc[index, :] - k_ch_data.loc[index - 1, :]) > 2)
            result2 = (abs(k_ch_data.loc[index, :] - k_ch_data.loc[index - 1, :]) > 4)

            if True in result1.values:
                merged_data.loc[index, 'my_flag'] = modify_flag(merged_data.loc[index, 'my_flag'], 3, '1')
            if True in result2.values:
                merged_data.loc[index, 'my_flag'] = modify_flag(merged_data.loc[index, 'my_flag'], 3, '2')

    def check_n5():
        """若超出阈值，则n5标记为1。"""

        def map_channel():
            """判断n5时，将不同通道数据映射为14通道用于判断阈值，返回映射后的通道列表"""
            freq_14 = []
            freq_n = []
            result = []
            for ch in get_config()["channels"]["14"]:
                freq_14.append(float(ch[3:]))
            for ch in ch_data.columns:
                freq_n.append(float(ch[3:]))
            freq_n = np.array(freq_n)
            # 分别寻找n通道中最接近14通道中每个通道的频率
            for freq in freq_14:
                temp = list(abs(freq_n - freq))
                result.append('Ch ' + format(freq_n[temp.index(min(temp))], '.3f'))
            return result

        # 若通道个数不为14，则映射为14通道
        if len(ch_data) != "14":
            channels = map_channel()
            mapped_data = ch_data.loc[:, channels]
        else:
            mapped_data = ch_data

        # 获得对应时间的阈值
        month = str(datetime.strptime(str(merged_data.loc[0, "datetime"]), '%Y-%m-%d %H:%M:%S').month)

        thresholds = get_config()["thresholds"]

        min_list = thresholds[month]['min']
        max_list = thresholds[month]['max']
        for index in merged_data.index:
            line = mapped_data.loc[index, :].reset_index(drop=True)
            # 如果有超出阈值的值
            if (False in (line >= min_list).values) | (False in (line < max_list).values):
                merged_data.loc[index, 'my_flag'] = modify_flag(merged_data.loc[index, 'my_flag'], 4, '1')

    # 入口
    db = Mysql()
    station_ids = db.get_station_id()

    for station_id in station_ids:
        # channel_map = db.get_channel_map(station_id)    # 数据库中的映射列表
        dataset = db.get_data(station_id)

        rollback_df = db.get_rollback_data(station_id)
        if not rollback_df.empty:
            rollback_df = spread_df(rollback_df)

        for df in dataset:
            # 拼接df和用于回溯的数据
            df = spread_df(df)
            merged_data = pd.concat([rollback_df, df], axis=0, ignore_index=True)
            merged_data['my_flag'] = '00000'
            # 提取通道数据
            ch_data = merged_data[get_channels(merged_data.columns)]

            # 质控n1
            check_n1()

            # 质控n2
            check_n2()
            # 由于回溯时无法完全质控每块前4数据，因此记录当前数据块(chunk)的最后四条质控码覆盖到下一个块的前四条，防止下一个数据块前四条质控码丢失
            try:
                merged_data.loc[:len(rollback_df) - 1, 'my_flag'] = last_3h_flag.values
            except UnboundLocalError:
                pass

            # 质控n3
            check_n3()

            # 质控n4
            check_n4()

            # 质控n5
            check_n5()

            print(merged_data)

            db.update_flag(merged_data[["station_id", "datetime", "is_rain", "my_flag"]])

            # 获取最后三小时的数据用于回溯
            t = df.loc[df.index[-1], "datetime"] - timedelta(minutes=180)
            rollback_df = df[df["datetime"] >= t]

            last_3h_flag = merged_data.iloc[-len(rollback_df):, 3]


def main():
    pd.set_option('display.max_columns', 100)
    pd.set_option('display.max_rows', 200)
    pd.set_option('display.width', 500)
    quality_control()


if __name__ == '__main__':
    main()
