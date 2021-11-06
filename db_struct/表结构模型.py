# 通道亮温数据表模拟ID
ids = [1, 4, 6, 10, 12, 15, 20, 21, 23, 26, 28, 30, 33, 34，36]

# lv1数据文件
class Lv1File(db.Model):
    __tablename__ = "t_lv1_file"
    id = db.Column(db.String(256), primary_key=True)
    # 微波辐射计设备逻辑外键
    wbfsj_id = db.Column(db.Integer, nullable=False)
    # 观测时间
    obs_time = db.Column(db.DateTime, index=True)
    file_path = db.Column(db.String(256))
    file_name = db.Column(db.String(256))
    isDelete = db.Column(db.Boolean, nullable=False)
    # 资源序列化-需要进行序列化的字段
    def keys(self):
        return ["id", "wbfsj_id", "obs_time", "file_path", "file_name"]

    def __getitem__(self, item):
        return getattr(self, item)


# lv1数据
class Lv1Data(db.Model):
    __tablename__ = "t_lv1_data"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    # 数据文件逻辑外键
    lv1_file_id = db.Column(db.String(256), nullable=False)
    datetime = db.Column(db.DateTime, index=True)
    temperature = db.Column(db.Float)
    humidity = db.Column(db.Float)
    pressure = db.Column(db.Float)
    tir = db.Column(db.Float)
    is_rain = db.Column(db.Integer)
    qcisDelete = db.Column(db.Integer)
    az = db.Column(db.Float)
    ei = db.Column(db.Float)
    qcisDelete_bt = db.Column(db.String(16))
    # 模拟亮温（json）:
    # [
    # 	{
    # 		“通道数据表ID”: 1,
    # 		 “亮温值”：24.345
    # 	},
    # 	{
    # 		“通道数据表ID”: 3，
    # 		 “亮温值”：27.334
    # 	},
    # 	...
    # ]
    brightness_emperature_43channels = db.Column(db.Text)
    def keys(self):
    isDelete = db.Column(db.Boolean, nullable=False)
    # 资源序列化-需要进行序列化的字段
        return [
            "id",
            "lv1_file_id",
            "datetime",
            "temperature",
            "humidity",
            "pressure",
            "tir",
            "is_rain",
            "qcisDelete",
            "az",
            "ei",
            "qcisDelete_bt",
            "brightness_emperature_43channels",
        ]

    def __getitem__(self, item):
        return getattr(self, item)

