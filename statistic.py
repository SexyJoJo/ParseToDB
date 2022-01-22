# -*- coding: utf-8 -*-
from qc_from_db import Mysql
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import calendar
from dateutil.relativedelta import relativedelta


class Statistic:
    @staticmethod
    def statistic(info):
        stime = info.loc[0, "datetime"]
        etime = info.loc[len(info) - 1, "datetime"]
        full_cnt = int((etime - stime) / timedelta(minutes=2)) + 1
        print(f"站台{station_id}：")
        print(f"    {stime}-{etime}总体应存在{full_cnt}条, 当前{len(info)}条"
              f",数据完整度{round(len(info) / full_cnt * 100, 2)}%")

        Statistic.statistic_by_month(info, stime, etime)

    @staticmethod
    def statistic_by_month(info, stime, etime):
        info.set_index("datetime", inplace=True)
        crr_time = stime
        Statistic.count_qc(info)
        while crr_time < etime:
            if crr_time == stime:
                month_start = crr_time
            else:
                month_start = Statistic.get_month_start(crr_time)
            month_end = Statistic.get_month_start(crr_time) + relativedelta(months=1)
            if crr_time.month == etime.month and crr_time.year == etime.year:
                month_end = etime

            total_cnt = int((month_end - month_start) / timedelta(minutes=2)) + 1

            info_bar = info.loc[month_start: month_end]
            print(f"        其中{month_start}-{month_end}应存在{total_cnt}条，当前{len(info_bar)}条,"
                  f"数据完整度{round(len(info_bar) / total_cnt * 100, 2)}%")

            crr_time = crr_time + relativedelta(months=1)

    @staticmethod
    def count_qc(df):
        if df.empty:
            pass
        else:
            err_cnt, unqc_cnt, pass_cnt = 0, 0, 0
            flag1_2_cnt = 0
            flag2_1_cnt, flag2_2_cnt = 0, 0
            flag3_1_cnt, flag3_2_cnt = 0, 0
            flag4_2_cnt = 0
            flag5_1_cnt = 0

            for index, row in df.iterrows():
                if row["my_flag"] != '00000':
                    err_cnt += 1
                    if row["my_flag"][0] == '2':
                        flag1_2_cnt += 1

                    if row["my_flag"][1] == '1':
                        flag2_1_cnt += 1
                    elif row["my_flag"][1] == '2':
                        flag2_2_cnt += 1
                    if row["my_flag"][2] == '1':
                        flag3_1_cnt += 1
                    elif row["my_flag"][2] == '2':
                        flag3_2_cnt += 1

                    if row["my_flag"][3] == '2':
                        flag4_2_cnt += 1

                    if row["my_flag"][4] == '1':
                        flag5_1_cnt += 1

                elif row["my_flag"] is None:
                    unqc_cnt += 1
                elif row["my_flag"] == '00000':
                    pass_cnt += 1

            print(f"        质控未通过个数{err_cnt}， 未质控个数{unqc_cnt}，质控通过个数{pass_cnt}")
            print(f"        质控通过率{round(err_cnt/len(df) * 100, 2)}%")
            print(f"        n1异常个数{flag1_2_cnt}")
            print(f"        n2疑似异常个数{flag2_1_cnt}, 异常个数{flag2_2_cnt}")
            print(f"        n3疑似异常个数{flag3_1_cnt}, 异常个数{flag3_2_cnt}")
            print(f"        n4异常个数{flag4_2_cnt}")
            print(f"        n5疑似异常个数{flag5_1_cnt}\n")

    @staticmethod
    def get_month_start(crr_time):
        """
        获取当前月的第一天
        :return:
        """
        year = crr_time.year
        month = crr_time.month
        if len(str(month)) == 1:
            month = '0' + str(month)
        ms = str(year) + "-" + str(month) + '-' + '01'
        ms = datetime.strptime(ms, "%Y-%m-%d")
        return ms


if __name__ == '__main__':
    db = Mysql()
    station_ids = db.get_station_id()
    for station_id in station_ids:
        statistic_info = db.get_statistic_info(station_id)
        if not statistic_info.empty:
            Statistic.statistic(statistic_info)
