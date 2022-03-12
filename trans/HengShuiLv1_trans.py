# -*- coding: utf-8 -*-
import os
import pandas as pd

PATH = r"D:\Data\microwave radiometer\Measured brightness temperature\衡水微波辐射计\data"

def lv2_to_THP(df):
    """提取lv2中的温、湿、压、降水标识要素，组成df"""
    df = df[df['10'] == 11].iloc[:, :7].drop(['10'], axis=1)
    df.columns = ['DateTime', 'SurTem(℃)', 'SurHum(%)', 'SurPre(hPa)', 'Tir(℃)', 'Rain']
    df['QCFlag'] = 0
    df['Az(deg)'] = 0
    df['El(deg)'] = 0
    return df


if __name__ == '__main__':
    for root, _, files in os.walk(PATH):
        for file in files:
            # 先读取亮温数据
            if file.endswith("LV1.csv"):
                try:
                    bt_df = pd.read_csv(os.path.join(root, file)).set_index('Record')\
                        .rename(columns={'Date/Time': 'DateTime'})
                except pd.errors.EmptyDataError:
                    print(f"文件{file}为空")
                    continue

                # 再读取温、湿、压、降水标识
                lv2_file = file[:-7] + 'LV2.csv'
                lv2_df = pd.read_csv(os.path.join(root, lv2_file), dtype={'Rain': str}).drop(0).set_index('Record')

                THP_df = lv2_to_THP(lv2_df)
                lv1_df = pd.merge(THP_df, bt_df, on='DateTime')
                lv1_df['QCFlag_BT'] = '00000'
                print(THP_df)
                print(lv1_df)
                break   # test
