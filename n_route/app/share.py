#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: jiaruya
@file: share.py
@time: 2019/08/28
@desc:
"""
import json
import logging.config

DEBUG = 1

# 配置文件目录
CFG_PATH = '../conf/'

# 日志配置
logging.config.fileConfig(CFG_PATH + 'logger.conf')
LOG = logging.getLogger('n_route')


# -----------------API接口返回值--------------------
RET_OK = json.dumps({'code': 0, 'data': '执行成功'})
RET_FAIL_PARAM = json.dumps({'code': 1, 'data': '参数错误'})
RET_FAIL_EXEC = json.dumps({'code': 2, 'data': '执行失败'})

# -----------------数据模式--------------------
Data_Mode_Dict = {'seq_sensor': 0, 'seq_event': 1, 'dis_sensor': 2, 'dis_event': 3}

# -----------------数据流向--------------------
Data_dir_Dict = {'up': 0, 'down': 1}

# -----------------传感类数据类型--------------------
Data_Type_Dict = {'double': 0, 'int64': 1, 'string': 2, 'bytes': 3}


# 传感类数据结构体
class Data:
    def __init__(self, tag, data_type, quality, timestamp,
                 value_double=0.0,
                 value_int64=0,
                 value_string='',
                 value_bytes=b''):
        # 标签点名称
        self.tag = tag
        # 值
        self.value_double = value_double
        self.value_int64 = value_int64
        self.value_string = value_string
        self.value_bytes = value_bytes
        # 数据类型
        self.data_type = data_type
        # 品质
        self.quality = quality
        # 时间戳
        self.timestamp = timestamp


# 事件类数据结构体
class Event:
    def __init__(self, source, event_type, level, keyword, content, timestamp):
        # 事件源
        self.source = source
        # 事件类型
        self.event_type = event_type
        # 等级
        self.level = level
        # 摘要
        self.keyword = keyword
        # 内容
        self.content = content
        # 时间戳
        self.timestamp = timestamp


# 加载python类库
def load_python(lib_name):
    try:
        lib = lib_name.split('.')[0]
        driver = __import__(lib)
        if hasattr(driver, 'DataDriver'):
            return driver.DataDriver()
        else:
            return None
    except ImportError as e:
        print(e)
        return None


# 加载c#类库
def load_csharp(lib_name):
    try:
        import clr
        clr.FindAssembly(lib_name)
		clr.AddReference(lib_name.split('.')[0])
        from DataDriver import DataDriver
        return DataDriver()
    except Exception as e:
        print(e)
        return None


# 读取配置文件
def load_config(file_name):
    try:
        file_name = CFG_PATH + file_name
        with open(file_name, 'r', encoding='UTF-8') as f:
            cfg_msg = json.loads(f.read())
        return cfg_msg
    except Exception:
        return {}


# 传感类数据对象转字典
def sensor2dict(data):
    if data:
        return {'tag': data.tag,
                'value_double': data.value_double,
                'value_string': data.value_string,
                'value_int64': data.value_int64,
                'value_bytes': data.value_bytes.decode(),
                'data_type': data.data_type,
                'quality': data.quality,
                'timestamp': data.timestamp}
    else:
        return {}


# 事件类数据对象转字典
def event2dict(event):
    if event:
        return {'source': event.source,
                'event_type': event.event_type,
                'level': event.level,
                'keyword': event.keyword,
                'content': event.content,
                'timestamp': event.timestamp}
    else:
        return {}


# 检查API执行返回结果
def check_api_result(ret):
    try:
        ret_dict = json.loads(ret, encoding='utf-8')
        if ret_dict['code'] == 0:
            return True
        else:
            return False
    except:
        return False


if __name__ == "__main__":
    load_csharp('s_opcda_client.dll')
