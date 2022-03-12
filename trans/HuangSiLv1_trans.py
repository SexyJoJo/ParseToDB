# -*- coding: utf-8 -*-
import pandas as pd
import os

PATH = r"D:\Data\microwave radiometer\Measured brightness temperature\皇寺微波辐射计\Data"

def split_file(path, filename):
    """
    皇寺lv1文件每两行为1条记录，需要拆分为两个文件，方便读取.
    将两个文件作为df拼接后返回
    """
    if not os.path.exists(r'./temp'):
        os.makedirs(r'./temp')

    with open(os.path.join(path, filename), 'r', encoding='gbk') as f:
        line_cnt = len(f.readlines())
        f.seek(0, 0)
        for i in range(int(line_cnt/2)):  # 获取文件行数的一半

            # 一行写入临时温湿压
            with open(r'./temp/thp.csv', 'a+', encoding='gbk') as thp:
                line = f.readline()
                thp.write(line)

            # 一行写入亮温
            with open(r'./temp/bt.csv', 'a+', encoding='gbk') as bt:
                line = f.readline()
                bt.write(line)

    thp_df = pd.read_csv(r'./temp/thp.csv', dtype=str)
    bt_df = pd.read_csv(r'./temp/bt.csv', dtype=str)
    merged_df = pd.merge(thp_df, bt_df, on='Date/Time')

    os.remove(r'./temp/thp.csv')
    os.remove(r'./temp/bt.csv')
    return merged_df


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


if __name__ == '__main__':
    for root, _, files in os.walk(PATH):
        for file in files:
            if file.endswith('LV1.csv'):
                # fullpath = os.path.join(root, file)
                try:
                    df = split_file(root, file)
                except FileNotFoundError:
                    print(f"文件{file}为空，跳过")
                    continue
                df = df.drop(['20', 'WindS', 'WindD', 'RainFall/M', 'RainFall/H', 'Record_y', '10', 'RainFall'], axis=1)
                df.rename(columns={'Record_x': 'Record',
                                   'Date/Time': 'DateTime',
                                   'Tamb(C)': 'SurTem(℃)',
                                   ' Rh(%)': 'SurHum(%)',
                                   ' Pres(hPa)': 'SurPre(hPa)',
                                   ' Tir(C)': 'Tir(℃)',
                                   ' Rain': 'Rain'}, inplace=True)
                # 替换列名
                columns = []
                for column in df.columns:
                    if column.startswith('Tsky'):
                        columns.append(f'Ch {column[4:]}')
                    else:
                        columns.append(column)
                df.columns = columns

                # 添加不存在的列
                df.insert(7, 'QCFlag', 0)
                df.insert(8, 'Az(deg)', 0)
                df.insert(9, 'El(deg)', 0)
                df['QCFlag_BT'] = '00000'

                ymd, ymd_hms = get_time(file)
                # 保存文件
                if not os.path.exists(rf'D:\Data\microwave radiometer\Measured brightness temperature\11111皇寺\{ymd}'):
                    os.makedirs(rf'D:\Data\microwave radiometer\Measured brightness temperature\11111皇寺\{ymd}')
                save_path = rf'D:\Data\microwave radiometer\Measured brightness temperature\11111皇寺\{ymd}\Z_UPAR_I_11111_{ymd_hms}_O_YMWR_FACTORY_RAW_D.txt'
                df.to_csv(save_path, sep=',', index=False)

                # 写入文件头
                with open(save_path, 'r+', encoding='utf-8') as f:
                    old = f.read()
                    f.seek(0, 0)
                    f.write('MWR,01.00\n')
                    f.write(f'11111,999.9999,99.9999,99.9,factory,99,99\n')
                    f.write(old)

                print(f"{file} 转换成功")
                # break
