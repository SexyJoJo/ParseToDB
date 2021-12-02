"""新格式文件质控"""
import numpy as np
import pandas as pd
import os
import json
import logging
import logging.config
from datetime import datetime, timedelta


class QualityControl:
    def __init__(self, data, yd_data, config):
        """读取当天与前一天数据，初始化QCFlag_BT,读取起始和结束通道以及K通道"""
        self.data = data
        self.yd_data = yd_data
        self.data.loc[:, 'QCFlag_BT'] = '00000'
        self.config = config
        self.channel_cnt = config["channel_cnt"]
        self.start_ch = config["channels"][config["channel_cnt"]][0]
        self.end_ch = config["channels"][config["channel_cnt"]][-1]
        self.thresholds = config["thresholds"]
        # 获取K通道起始结束位置
        self.kch = []
        for column in data.loc[:, self.start_ch:self.end_ch].columns:
            if config["k_range"][0] <= float(column[2:]) <= config["k_range"][1]:
                self.kch.append(column)

    def check_n1(self):
        """若任意通道亮温≤0K或>400K或为空值，则该时次质量标识n1标记为2"""
        chdata = self.data.loc[:, self.start_ch:self.end_ch]
        chdata = chdata.astype('float64')
        result = ((chdata > 400) | (chdata <= 0)) | chdata.isnull()
        # 获得需要更改QCFlag_BT的索引列表
        modi_index = chdata[result].dropna(how='all').index
        self.data.loc[modi_index, 'QCFlag_BT'] =\
            self.data.loc[modi_index, 'QCFlag_BT'].apply(self.__modify_flag, index=0, val='2')

    def check_n2(self):
        """n2 : 若任意通道亮温连续3个数值保持不变（不考虑时间间隔），则该时次质量标识n2标记为1;（回溯当前文件前一天最新的2条记录）
        若任意通道亮温连续5个数值保持不变（不考虑时间间隔），则该时次质量标识n2标记为2;（回溯当前文件前一天最新的4条记录）

        1.前一天数据不存在
            1）今日数据条数 n < 3  -->  直接跳过
            2）今日数据条数 n >= 3  -->  在今日数据前拼接4条空数据
        2.前一天数据存在
            1）前一天数据条数 n >= 4  -->  正常质控
            2）前一天数据条数 n < 4  -->  拼接，再拼接 4-n 条空数据"""
        # 拼接前一天后4条与当天的通道数据用于回溯
        chdata = self.data.loc[:, self.start_ch:self.end_ch]
        chdata = chdata.astype('float64')
        if self.yd_data.empty:
            if len(self.data) < 3:
                return
            else:
                temp = pd.DataFrame(columns=chdata.columns, index=[x for x in range(0, 4)])
                merged_chdata = pd.concat([temp, chdata], axis=0).reset_index().iloc[:, 1:]
        else:
            yd_chdata = self.yd_data.loc[len(self.yd_data)-4:, self.start_ch:self.end_ch]
            yd_chdata = yd_chdata.astype('float64')
            len_yd_data = len(yd_chdata)
            if len_yd_data >= 4:
                merged_chdata = pd.concat([yd_chdata, chdata], axis=0).reset_index().iloc[:, 1:]
            else:
                print("拼接")
                temp = pd.DataFrame(columns=chdata.columns, index=[x for x in range(0, 4-len_yd_data)])
                temp = pd.concat([temp, yd_chdata], axis=0).reset_index().iloc[:, 1:]
                merged_chdata = pd.concat([temp, chdata], axis=0).reset_index().iloc[:, 1:]

        for column in merged_chdata.columns:
            values = merged_chdata[column]  # 每列的数值列表
            for index in range(4, len(merged_chdata)):
                # 检查n2是否经过前面通道的更改已经变为2，如果已经为2则检查下一行
                if self.data.loc[index - 4, 'QCFlag_BT'][1] == '2':
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
                    self.data.loc[index - 4, 'QCFlag_BT'] = \
                        self.__modify_flag(self.data.loc[index - 4, 'QCFlag_BT'], 1, '2')
                elif 3 <= cnt < 5:
                    self.data.loc[index - 4, 'QCFlag_BT'] = \
                        self.__modify_flag(self.data.loc[index - 4, 'QCFlag_BT'], 1, '1')

    def check_n3(self):
        chdata = self.data.loc[:, self.start_ch:self.end_ch]
        chdata = chdata.astype('float64')

        # 设备自带降水标记判别：当降水标识为1，则该时次质量标识n3标记为2
        self.data.loc[self.data['Rain'] == 1, 'QCFlag_BT'] = \
            self.data.loc[self.data['Rain'] == 1, 'QCFlag_BT'].apply(self.__modify_flag, index=2, val='2')

        # 若第1通道(22GHz左右）亮温＞169K，则该时次质量标识n3标记为1，降水标识（不改原文件，仅指中间过程文件）为1
        ch1 = chdata.loc[:, self.start_ch]
        result = (ch1 > 169)
        self.data.loc[result, 'Rain'] = 1
        result = (ch1 > 169) & (self.data.loc[:, 'QCFlag_BT'].apply(lambda flag: flag[2]) != '2')  # 若n3已经为2，则无需更改
        self.data.loc[result, 'QCFlag_BT'] =\
            self.data.loc[result, 'QCFlag_BT'].apply(self.__modify_flag, index=2, val='1')

        # 当降雨标识为0，且前一个降雨标识为1时，若各通道亮温观测值较前一个时次下降，则将该时次n3标记为1，降水标识为1；
        for index in self.data.index:
            # 若n3已经为2或者1，或者前一天数据不存在则判断下一行
            if self.data.loc[index, 'QCFlag_BT'][2] != '0' or self.yd_data.empty:
                continue
            # 判断当天第一行时需要回溯前一天的最后一行
            if index == 0:
                if (self.data.loc[index, 'Rain'] == 0) & (self.yd_data.loc[len(self.yd_data)-1, 'Rain'] == 1):
                    result = (chdata.loc[index, :] < self.yd_data.loc[len(self.yd_data)-1, self.start_ch:self.end_ch])
                    if False in result.values:  # 没有全部下降
                        continue
                    else:
                        self.data.loc[index, 'Rain'] = 1
                        self.data.loc[index, 'QCFlag_BT'] = \
                            self.__modify_flag(self.data.loc[index, 'QCFlag_BT'], 2, '1')
            # 第二行到最后
            else:
                if (self.data.loc[index, 'Rain'] == 0) & (self.data.loc[index - 1, 'Rain'] == 1):
                    # 统计下降通道的个数
                    result = (chdata.loc[index, :] < chdata.loc[index - 1, :])
                    if False in result.values:  # 没有全部下降
                        continue
                    else:
                        self.data.loc[index, 'Rain'] = 1
                        self.data.loc[index, 'QCFlag_BT'] = \
                            self.__modify_flag(self.data.loc[index, 'QCFlag_BT'], 2, '1')

        # 当中间文件的降水标识为1，计算与最近一次标识为1的降雨的观测时间差，若小于3小时，则将两次降雨标识之间的数据n3标识为1。
        if not self.yd_data.empty:
            self.yd_data.loc[:, 'QCFlag_BT'] = '00000'
        if not self.data[self.data['Rain'] == 1].empty:  # 若不存在Rain为1的记录，则跳过此步
            for index in self.data[self.data['Rain'] == 1].index:
                # rain为1的第一行数据需要回溯前一天最后一条rain为1的记录
                if index == self.data[self.data['Rain'] == 1].index[0]:
                    pre_index = index  # 初始化pre_index，用于无需回溯时，记录index的前一个rain为1的位置

                    # 若前一天不存在rain为1，则无需回溯
                    if not self.yd_data.empty:
                        if not self.yd_data[self.yd_data['Rain'] == 1].empty:
                            yd_index = self.yd_data[self.yd_data['Rain'] == 1].index[-1]  # 前一天最后一个rain为1的行索引
                            t1 = datetime.strptime(self.yd_data.loc[yd_index, 'DateTime'], '%Y-%m-%d %H:%M:%S')
                            t2 = datetime.strptime(self.data.loc[index, 'DateTime'], '%Y-%m-%d %H:%M:%S')
                            if (t2 - t1).seconds / 3600 < 3:
                                # 处理边界情况
                                if yd_index != len(self.yd_data)-1:
                                    self.yd_data.loc[yd_index+1:, 'QCFlag_BT'] = \
                                        self.yd_data.loc[yd_index+1:, 'QCFlag_BT']\
                                            .apply(self.__modify_flag, index=2, val='1')
                                if index != 0:
                                    self.data.loc[:index-1, 'QCFlag_BT'] = \
                                        self.data.loc[:index-1, 'QCFlag_BT']\
                                            .apply(self.__modify_flag, index=2, val='1')
                # 无需回溯部分
                else:
                    t1 = datetime.strptime(self.data.loc[pre_index, 'DateTime'], '%Y-%m-%d %H:%M:%S')
                    t2 = datetime.strptime(self.data.loc[index, 'DateTime'], '%Y-%m-%d %H:%M:%S')
                    # 间隔小于3小时，将pre_index 和 index 之间的n3标为1
                    if (t2 - t1).seconds / 3600 < 3:
                        self.data.loc[pre_index+1:index-1, 'QCFlag_BT'] = \
                            self.data.loc[pre_index+1:index-1, 'QCFlag_BT']\
                                .apply(self.__modify_flag, index=2, val='1')
                    pre_index = index  # 向下挪动pre_index

    def check_n4(self):
        """K通道观测亮温与前一个亮温值差值的绝对值≤2K，则通过一致性检验，质量标识n4标记为0
        若差值绝对值>2K且≤4K，质量标识n4标记为1；若差值绝对值>4K，则质量标识n4标记为2。"""
        chdata = self.data.loc[:, self.kch[0]:self.kch[-1]]
        chdata = chdata.astype('float64')
        if len(chdata <= 1):
            return

        for index in self.data.index:
            # 需要回溯前一天最后一行
            if index == 0 and not self.yd_data.empty:
                result1 = (abs(chdata.loc[index, :] -
                               self.yd_data.loc[len(self.yd_data)-1, self.kch[0]:self.kch[-1]]) <= 4) \
                          & (abs(chdata.loc[index, :] -
                                 self.yd_data.loc[len(self.yd_data)-1, self.kch[0]:self.kch[-1]]) > 2)
                result2 = (abs(chdata.loc[index, :] -
                               self.yd_data.loc[len(self.yd_data)-1, self.kch[0]:self.kch[-1]]) > 4)

            # 无需回溯部分
            else:
                result1 = (abs(chdata.loc[index, :] - chdata.loc[index-1, :]) <= 4) & \
                         (abs(chdata.loc[index, :] - chdata.loc[index-1, :]) > 2)
                result2 = (abs(chdata.loc[index, :] - chdata.loc[index-1, :]) > 4)

            if True in result1.values:
                self.data.loc[index, 'QCFlag_BT'] = \
                    self.__modify_flag(self.data.loc[index, 'QCFlag_BT'], 3, '1')
            if True in result2.values:
                self.data.loc[index, 'QCFlag_BT'] = \
                    self.__modify_flag(self.data.loc[index, 'QCFlag_BT'], 3, '2')

    def check_n5(self):
        """若超出阈值，则n5标记为1。"""
        if self.channel_cnt != "14":
            channels = self.__map_channel()
            chdata = self.data.loc[:, channels]
        else:
            chdata = self.data.loc[:, self.start_ch:self.end_ch]
        chdata = chdata.astype('float64')

        # 获得对应时间的阈值
        month = str(datetime.strptime(self.data.iloc[0, 1], '%Y-%m-%d %H:%M:%S').month)

        min_list = self.thresholds[month]['min']
        max_list = self.thresholds[month]['max']
        for index in self.data.index:
            line = chdata.loc[index, :].reset_index(drop=True)
            # 如果有超出阈值的值
            if (False in (line >= min_list).values) | (False in (line < max_list).values):
                self.data.loc[index, 'QCFlag_BT'] = self.__modify_flag(self.data.loc[index, 'QCFlag_BT'], 4, '1')

    def save_file(self):
        # out_path = os.path.splitext(self.config["filepath"])[0] + r'_QC.csv'
        self.data.to_csv(full_path, encoding='gbk', index=False)

    @staticmethod
    def __modify_flag(flag, index, val):
        """将flag中的index位替换为其他数值val"""
        s = list(flag)
        s[index] = val
        flag = ''.join(s)
        return flag

    def __map_channel(self):
        """判断n5时，将不同通道数据映射为14通道用于判断阈值，返回映射后的通道列表"""
        freq_14 = []
        freq_n = []
        result = []
        for ch in self.config["channels"]["14"]:
            freq_14.append(float(ch[2:]))
        for ch in self.config["channels"][self.channel_cnt]:
            freq_n.append(float(ch[2:]))
        freq_n = np.array(freq_n)
        # 分别寻找n通道中最接近14通道中每个通道的频率
        for freq in freq_14:
            temp = list(abs(freq_n - freq))
            result.append('Ch ' + format(freq_n[temp.index(min(temp))], '.3f'))
        return result


def main(config_path, filepath):
    # 载入日志配置
    with open(r"config/quality_control/log_config.json", "r") as config_json:
        log_config = json.load(config_json)
    logging.config.dictConfig(log_config)
    logger = logging.getLogger("root")
    # try:
    # 提取配置信息
    with open(config_path, "r") as config_json:
        config = json.load(config_json)
    # filepath = config["filepath"]

    # 获得前一天文件的路径用于回溯
    path, td_file = os.path.split(filepath)
    date = td_file.split("_")[4]
    td = datetime.strptime(date, '%Y%m%d%H%M%S')
    yd = datetime.strftime(td - timedelta(days=1), '%Y%m%d%H%M%S')
    field[4] = yd
    temp = path.split('\\')
    temp[-1] = yd[:8]
    yd_path = '\\'.join(temp)
    yd_file = "_".join(field)
    yd_filepath = os.path.join(yd_path, yd_file)

    # 读取文件
    # try:
    data = pd.read_csv(filepath, header=2, engine="python", encoding="gbk")
    data = data.replace(["\\\\", "\\", "/"], -999)
    try:
        yd_data = pd.read_csv(yd_filepath, header=2, engine="python", encoding="gbk")
        yd_data = yd_data.replace(["\\\\", "\\", "/"], -999)
    except FileNotFoundError:
        yd_data = pd.DataFrame()

    # 质控
    qc = QualityControl(data.copy(), yd_data, config)
    qc.check_n1()
    qc.check_n2()
    qc.check_n3()
    qc.check_n4()
    qc.check_n5()
    qc.save_file()
    print(qc.data)
    return True

    # except Exception as e:
    #     print(e)
    #     logger.error(e)


if __name__ == '__main__':
    with open("config/quality_control/qc_file_config.json", "r", encoding='utf-8') as config:
        qc_dir = json.load(config)["dir_path"]
    for root, dirs, files in os.walk(qc_dir):
        for file in files:
            field = file.split('_')
            try:
                if field[5] == 'P' or field[-1][0] == 'M' or field[-1][2:] != 'txt' or field[-2] in ['CAL', 'STA']:
                    continue
            except Exception:
                continue
            full_path = os.path.join(root, file)
            full_path = full_path.replace('\\\\', '\\')
            print("正在质控：", full_path)

            # 读取开头两行设备信息，用于后续写入
            with open(full_path, "r", encoding='gbk') as f:
                line1 = f.readline()
                line2 = f.readline()

            # 质控
            result = main(config_path=r"config/quality_control/qc_file_config.json", filepath=full_path)

            # 在文件开头写入设备信息
            if result:
                with open(full_path, "r+", encoding='gbk') as f:
                    old = f.read()
                    f.seek(0, 0)
                    f.write(line1)
                    f.write(line2)
                    f.write(old)

