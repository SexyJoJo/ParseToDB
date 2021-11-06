# Lv1 csv旧格式格式文件名匹配正则：
LV1_CSV_RE_STRING = (
    "^[0-9]{5}-[A-Za-z]{2}-[0-9]{4}-[0-9]{2}-[0-9]{2}[Ll][Vv][1].*?.[Cc][Ss][Vv]$"
)
# Lv1 txt格式文件名匹配正则：
LV1_TXT_RE_STRING = "^[A-Za-z]_[A-Za-z]{4}_[A-Za-z]_[0-9]{5}_[0-9]{14}.*?.[Tt][Xx][Tt]"
# csv旧格式的lv1文件表头常量
LV1_CSV_RECORD = "record"
LV1_CSV_TIME_FORMAT = "%Y/%m/%d %H:%M"
LV1_CSV_TIME = "Date/Time"
LV1_CSV_TEMP = "Temp(C)"
LV1_CSV_HUMIDITY = "RH(%)"
LV1_CSV_PRESSURE = "Pres(hPa)"
LV1_CSV_TIR = "Tir(c)"
LV1_CSV_RAIN = "Rain"
"""
旧格式文件统一替换表头
"""
LV1_CSV_UNIFIED_HEADER = [
    LV1_CSV_TIME,
    LV1_CSV_TEMP,
    LV1_CSV_HUMIDITY,
    LV1_CSV_PRESSURE,
    LV1_CSV_TIR,
    LV1_CSV_RAIN,
]



# TXT格式的lv1文件表头常量
LV1_TXT_RECORD = "Record"
LV1_TXT_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
LV1_TXT_TIME = "DateTime"
# LV1_TXT_TEMP = "SurTem(°C)"
LV1_TXT_TEMP = "SurTem(℃)"
LV1_TXT_HUMIDITY = "SurHum(%)"
LV1_TXT_PRESSURE = "SurPre(hPa)"
# LV1_TXT_TIR = "Tir(°C)"
LV1_TXT_TIR = "Tir(℃)"
LV1_TXT_RAIN = "Rain"
LV1_TXT_QCFLAG = "QCFlag"
LV1_TXT_AZ = "Az(deg)"
LV1_TXT_EI = "El(deg)"
LV1_TXT_QCFlag_BT = "QCFlag_BT"

"""
新格式文件统一替换表头字段
"""
LV1_TXT_UNIFIED_HEADER = [
    LV1_TXT_TIME,
    LV1_TXT_TEMP,
    LV1_TXT_HUMIDITY,
    LV1_TXT_PRESSURE,
    LV1_TXT_TIR,
    LV1_TXT_RAIN,
    LV1_TXT_QCFLAG,
    LV1_TXT_AZ,
    LV1_TXT_EI,
]