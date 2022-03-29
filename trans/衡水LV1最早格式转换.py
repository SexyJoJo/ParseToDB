# -*- coding: utf-8 -*-
import os
import pandas as pd

PATH = r"D:\Data\microwave radiometer\Measured brightness temperature\衡水微波辐射计\data最早"
CHS = ['Ch 22.235',
       'Ch 22.500',
       'Ch 23.035',
       'Ch 23.835',
       'Ch 25.000',
       'Ch 26.235',
       'Ch 28.000',
       'Ch 30.000',
       'Ch 51.250',
       'Ch 51.760',
       'Ch 52.280',
       'Ch 52.800',
       'Ch 53.340',
       'Ch 53.850',
       'Ch 54.400',
       'Ch 54.940',
       'Ch 55.500',
       'Ch 56.020',
       'Ch 56.660',
       'Ch 57.290',
       'Ch 57.960',
       'Ch 58.800']

def get_time(filename):
    """
    获取文件名中的时间信息
    文件名样例：ZP2018-10-30_12-36-42LV1.csv
    """
    year = filename[2:6]
    month = filename[7:9]
    day = filename[10:12]
    hour = filename[13:15]
    minute = filename[16:18]
    second = filename[19:21]
    ymd = year + month + day
    ymd_hms = ymd + hour + minute + second
    return ymd, ymd_hms

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
                    bt_df = pd.read_csv(os.path.join(root, file), dtype=str) \
                        .rename(columns={'Date/Time': 'DateTime'})
                except pd.errors.EmptyDataError:
                    print(f"文件{file}为空")
                    continue

                # 再读取温、湿、压、降水标识
                lv2_file = file[:-7] + 'LV2.csv'
                lv2_df = pd.read_csv(os.path.join(root, lv2_file), dtype={'Rain': str}).drop(0).set_index('Record')

                THP_df = lv2_to_THP(lv2_df)
                lv1_df = pd.merge(THP_df, bt_df, on='DateTime')

                Record = lv1_df['Record']
                lv1_df = lv1_df.drop('Record', axis=1)
                lv1_df.insert(0, 'Record', Record)

                # 替换列名
                columns = []
                for column in lv1_df.columns:
                    if not column.startswith('Tsky'):
                        columns.append(column)
                columns = columns + CHS
                lv1_df.columns = columns
                lv1_df['QCFlag_BT'] = '00000'

                print(THP_df)
                print(lv1_df)

                ymd, ymd_hms = get_time(file)
                # 保存文件
                if not os.path.exists(rf'D:\Data\microwave radiometer\Measured brightness temperature\54702衡水\{ymd}'):
                    os.makedirs(rf'D:\Data\microwave radiometer\Measured brightness temperature\54702衡水\{ymd}')
                save_path = rf'D:\Data\microwave radiometer\Measured brightness temperature\54702衡水\{ymd}\Z_UPAR_I_54702_{ymd_hms}_O_YMWR_BFTQ_RAW_D.txt'
                lv1_df.to_csv(save_path, sep=',', index=False)

                # 写入文件头
                with open(save_path, 'r+', encoding='utf-8') as f:
                    old = f.read()
                    f.seek(0, 0)
                    f.write('MWR,01.00\n')
                    f.write(f'54702,115.7111,37.7169,35.7,BFTQ,22,83\n')
                    f.write(old)

                print(f"{file} 转换成功")