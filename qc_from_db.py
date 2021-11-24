# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy
import uuid
import json
import numpy as np


class Mysql:
    def __init__(self):
        self.conn = sqlalchemy.create_engine('mysql+pymysql://root:123@localhost/microwavw_newdata?charset=utf8')
        self.condition = get_config()["select_condition"]

    def read_df(self, date):
        """根据设备号、日期、生产厂家，查询数据库，作为dataframe返回"""
        file_id = uuid.uuid3(uuid.NAMESPACE_DNS, self.condition["dev_id"] + date + self.condition["factory"])
        sql = """SELECT datetime, is_rain, brightness_emperature_43channels, qcisDelete_bt
                 FROM t_lv1_data WHERE lv1_file_id = '%s'""" % file_id
        df = pd.read_sql(sql, self.conn)
        return df


class QualityControl:
    def __init__(self):
        self.channels = None
        self.ch_cnt = None

    def __map_channel(self):
        """判断n5时，将不同通道数据映射为14通道用于判断阈值，返回映射后的通道列表"""
        freq_14 = []
        freq_n = []
        result = []
        for ch in get_config()["channels"]["14"]:
            freq_14.append(float(ch[2:]))
        for ch in get_config()["channels"][str(self.ch_cnt)]:
            freq_n.append(float(ch[2:]))
        freq_n = np.array(freq_n)
        # 分别寻找n通道中最接近14通道中每个通道的频率
        for freq in freq_14:
            temp = list(abs(freq_n - freq))
            result.append('Ch ' + format(freq_n[temp.index(min(temp))], '.3f'))
        return result

    def check_n1(self, data):
        """若任意通道亮温≤0K或>400K或为空值，则该时次质量标识n1标记为2"""
        chdata = data.loc[:, self.channels[0]: self.channels[-1]]
        result = ((chdata > 400) | (chdata <= 0)) | chdata.isnull()
        # 获得需要更改QCFlag_BT的索引列表
        modi_index = chdata[result].dropna(how='all').index
        data.loc[modi_index, 'qcisDelete_bt'] = \
            data.loc[modi_index, 'qcisDelete_bt'].apply(modify_flag, index=0, val='2')
        return data

    def check_n2(self, data, yd_data):
        """n2 : 若任意通道亮温连续3个数值保持不变（不考虑时间间隔），则该时次质量标识n2标记为1;（回溯当前文件前一天最新的2条记录）
        若任意通道亮温连续5个数值保持不变（不考虑时间间隔），则该时次质量标识n2标记为2;（回溯当前文件前一天最新的4条记录）"""
        # 拼接前一天后4条与当天的通道数据用于回溯
        chdata = data.loc[:, self.channels[0]:self.channels[-1]]
        yd_chdata = yd_data.loc[len(yd_data)-4:, self.channels[0]:self.channels[-1]]
        merged_chdata = pd.concat([yd_chdata, chdata], axis=0).reset_index().iloc[:, 1:]

        for column in merged_chdata.columns:
            values = merged_chdata[column]  # 每列的数值列表
            for index in range(4, len(merged_chdata)):
                # 检查n2是否经过前面通道的更改已经变为2，如果已经为2则检查下一行
                if data.loc[index - 4, 'qcisDelete_bt'][1] == '2':
                    continue
                # 统计重复个数
                cnt = 1
                i = index
                while abs(values[i] - values[i-1]) < 0.001:  # 当前值较前一个值不变
                    cnt += 1
                    i -= 1
                    if cnt == 5:
                        break
                # 更改QCFlag_BT中的n2
                if cnt == 5:
                    data.loc[index - 4, 'qcisDelete_bt'] = \
                        modify_flag(data.loc[index - 4, 'qcisDelete_bt'], 1, '2')
                elif 3 <= cnt < 5:
                    data.loc[index - 4, 'qcisDelete_bt'] = \
                        modify_flag(data.loc[index - 4, 'qcisDelete_bt'], 1, '1')
                return data

    def check_n3(self, data, yd_data):
        chdata = data.loc[:, self.channels[0]:self.channels[-1]]

        # 设备自带降水标记判别：当降水标识为1，则该时次质量标识n3标记为2
        data.loc[data['is_rain'] == 1, 'qcisDelete_bt'] = \
            data.loc[data['is_rain'] == 1, 'qcisDelete_bt'].apply(modify_flag, index=2, val='2')

        # 若第1通道(22GHz左右）亮温＞169K，则该时次质量标识n3标记为1，降水标识（不改原文件，仅指中间过程文件）为1
        ch1 = chdata.loc[:, self.channels[0]]
        result = (ch1 > 169)
        data.loc[result, 'is_rain'] = 1
        result = (ch1 > 169) & (data.loc[:, 'qcisDelete_bt'].apply(lambda flag: flag[2]) != '2')  # 若n3已经为2，则无需更改
        data.loc[result, 'qcisDelete_bt'] =\
            data.loc[result, 'qcisDelete_bt'].apply(modify_flag, index=2, val='1')

        # 当降雨标识为0，且前一个降雨标识为1时，若各通道亮温观测值较前一个时次下降，则将该时次n3标记为1，降水标识为1；
        for index in data.index:
            # 若n3已经为2或者1，则判断下一行
            if data.loc[index, 'qcisDelete_bt'][2] != '0':
                continue
            # 判断当天第一行时需要回溯前一天的最后一行
            if index == 0:
                if (data.loc[index, 'is_rain'] == 0) & (yd_data.loc[len(yd_data)-1, 'is_rain'] == 1):
                    result = (chdata.loc[index, :] < yd_data.loc[len(yd_data)-1, self.channels[0]:self.channels[-1]])
                    if False in result.values:  # 没有全部下降
                        continue
                    else:
                        data.loc[index, 'is_rain'] = 1
                        data.loc[index, 'qcisDelete_bt'] = \
                            modify_flag(data.loc[index, 'qcisDelete_bt'], 2, '1')
            # 第二行到最后
            else:
                if (data.loc[index, 'is_rain'] == 0) & (data.loc[index - 1, 'is_rain'] == 1):
                    # 统计下降通道的个数
                    result = (chdata.loc[index, :] < chdata.loc[index - 1, :])
                    if False in result.values:  # 没有全部下降
                        continue
                    else:
                        data.loc[index, 'is_rain'] = 1
                        data.loc[index, 'qcisDelete_bt'] = \
                            modify_flag(data.loc[index, 'qcisDelete_bt'], 2, '1')

        # 当中间文件的降水标识为1，计算与最近一次标识为1的降雨的观测时间差，若小于3小时，则将两次降雨标识之间的数据n3标识为1。
        yd_data.loc[:, 'qcisDelete_bt'] = '00000'
        if not data[data['is_rain'] == 1].empty:  # 若不存在Rain为1的记录，则跳过此步
            for index in data[data['is_rain'] == 1].index:
                # rain为1的第一行数据需要回溯前一天最后一条rain为1的记录
                if index == data[data['is_rain'] == 1].index[0]:
                    pre_index = index  # 初始化pre_index，用于无需回溯时，记录index的前一个rain为1的位置
                    if not yd_data[yd_data['is_rain'] == 1].empty:  # 若前一天不存在rain为1，则无需回溯
                        yd_index = yd_data[yd_data['is_rain'] == 1].index[-1]  # 前一天最后一个rain为1的行索引
                        t1 = datetime.strptime(str(yd_data.loc[yd_index, 'datetime']), '%Y-%m-%d %H:%M:%S')
                        t2 = datetime.strptime(str(data.loc[index, 'datetime']), '%Y-%m-%d %H:%M:%S')
                        if (t2 - t1).seconds / 3600 < 3:
                            # 处理边界情况
                            if yd_index != len(yd_data)-1:
                                yd_data.loc[yd_index+1:, 'qcisDelete_bt'] = \
                                    yd_data.loc[yd_index+1:, 'qcisDelete_bt'].apply(modify_flag, index=2, val='1')
                            if index != 0:
                                data.loc[:index-1, 'qcisDelete_bt'] = \
                                    data.loc[:index-1, 'qcisDelete_bt'].apply(modify_flag, index=2, val='1')
                # 无需回溯部分
                else:
                    t1 = datetime.strptime(str(data.loc[pre_index, 'datetime']), '%Y-%m-%d %H:%M:%S')
                    t2 = datetime.strptime(str(data.loc[index, 'datetime']), '%Y-%m-%d %H:%M:%S')
                    # 间隔小于3小时，将pre_index 和 index 之间的n3标为1
                    if (t2 - t1).seconds / 3600 < 3:
                        data.loc[pre_index+1:index-1, 'qcisDelete_bt'] = \
                            data.loc[pre_index+1:index-1, 'qcisDelete_bt'].apply(modify_flag, index=2, val='1')
                    pre_index = index  # 向下挪动pre_index
        return data, yd_data

    def check_n4(self, data, yd_data):
        """K通道观测亮温与前一个亮温值差值的绝对值≤2K，则通过一致性检验，质量标识n4标记为0
        若差值绝对值>2K且≤4K，质量标识n4标记为1；若差值绝对值>4K，则质量标识n4标记为2。"""
        def get_kch():
            k_channels = []
            k_range = get_config()["k_range"]
            for column in data.loc[:, self.channels[0]:self.channels[-1]].columns:
                if k_range[0] <= float(column[2:]) <= k_range[1]:
                    k_channels.append(column)
            return k_channels

        kch = get_kch()
        chdata = data.loc[:, kch[0]: kch[-1]]

        for index in data.index:
            # 需要回溯前一天最后一行
            if index == 0:
                result1 = (abs(chdata.loc[index, :] -
                               yd_data.loc[len(yd_data)-1, kch[0]: kch[-1]]) <= 4) \
                          & (abs(chdata.loc[index, :] -
                                 yd_data.loc[len(yd_data)-1, kch[0]: kch[-1]]) > 2)
                result2 = (abs(chdata.loc[index, :] -
                               yd_data.loc[len(yd_data)-1, kch[0]: kch[-1]]) > 4)

            # 无需回溯部分
            else:
                result1 = (abs(chdata.loc[index, :] - chdata.loc[index-1, :]) <= 4) & \
                         (abs(chdata.loc[index, :] - chdata.loc[index-1, :]) > 2)
                result2 = (abs(chdata.loc[index, :] - chdata.loc[index-1, :]) > 4)

            if True in result1.values:
                data.loc[index, 'qcisDelete_bt'] = modify_flag(data.loc[index, 'qcisDelete_bt'], 3, '1')
            if True in result2.values:
                data.loc[index, 'qcisDelete_bt'] = modify_flag(data.loc[index, 'qcisDelete_bt'], 3, '2')

        return data

    def check_n5(self, data):
        """若超出阈值，则n5标记为1。"""
        if self.ch_cnt != "14":
            channels = self.__map_channel()
            chdata = data.loc[:, channels]
        else:
            chdata = data.loc[:, self.channels[0]:self.channels[-1]]
        # 获得对应时间的阈值
        month = str(datetime.strptime(str(data.iloc[0, 0]), '%Y-%m-%d %H:%M:%S').month)

        thresholds = get_config()["thresholds"]

        min_list = thresholds[month]['min']
        max_list = thresholds[month]['max']
        for index in data.index:
            line = chdata.loc[index, :].reset_index(drop=True)
            # 如果有超出阈值的值
            if (False in (line >= min_list).values) | (False in (line < max_list).values):
                data.loc[index, 'qcisDelete_bt'] = modify_flag(data.loc[index, 'qcisDelete_bt'], 4, '1')
        return data

    def lv1_qc(self):
        """lv1质控"""
        def spread_df(df):
            """将json格式的通道数据展开为dataframe, 将df和通道个数一并返回"""
            # 将原json转换为pd.read_json能够读取的形式
            ch_data = json.dumps([json.loads(data) for data in df["brightness_emperature_43channels"]])
            ch_data = pd.read_json(ch_data)
            self.ch_cnt = len(ch_data.columns)  # 获取通道个数
            self.channels = ch_data.columns  # 获取通道列表

            del df["brightness_emperature_43channels"]
            df["qcisDelete_bt"] = '00000'
            df = pd.concat([df, ch_data], axis=1)
            return df

        db = Mysql()
        # 读取待质控时间范围
        date_range = get_config()["date_range"]
        today = date_range["start"]

        # 主循环
        while today != date_range["end"]:
            print("-------------------------")
            print(f"正在质控{today}日文件")
            yesterday_df = db.read_df(get_yesterday(today))
            today_df = db.read_df(today)
            if yesterday_df.empty:
                print("前一日文件不存在,跳过质控")
            else:
                # 取得用来完成所有质控的材料
                yesterday_df = spread_df(yesterday_df)  # 用于回溯
                today_df = spread_df(today_df)

                # 正式质控
                today_df = self.check_n1(today_df)
                today_df = self.check_n2(today_df, yesterday_df)
                today_df, yesterday_df = self.check_n3(today_df, yesterday_df)
                today_df = self.check_n4(today_df, yesterday_df)
                today_df = self.check_n5(today_df)
                print(today_df)

            # 处理下一天
            today = get_tomorrow(today)

            break  # test

def get_config():
    with open("config/quality_control/qc_db_config.json", 'r') as f:
        config = json.load(f)
    return config

def get_tomorrow(date):
    """获取date后一天的日期"""
    today = datetime.strptime(date, '%Y%m%d')
    tomorrow = datetime.strftime(today + timedelta(days=1), '%Y%m%d')
    return tomorrow

def get_yesterday(date):
    today = datetime.strptime(date, '%Y%m%d')
    yesterday = datetime.strftime(today - timedelta(days=1), '%Y%m%d')
    return yesterday

def modify_flag(flag, index, val):
    """将flag中的index位替换为其他数值val"""
    s = list(flag)
    s[index] = val
    flag = ''.join(s)
    return flag


if __name__ == '__main__':
    # 设置打印宽度
    pd.set_option('display.max_columns', 20)
    pd.set_option('display.width', 1000)

    qc = QualityControl()
    qc.lv1_qc()

