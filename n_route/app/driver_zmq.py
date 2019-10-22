#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: jiaruya
@file: driver_zmq.py
@time: 2019/09/01
@desc:
"""
import zmq
import json
import time
import logging
import threading
from share import Data, Event
from share import RET_OK, RET_FAIL_PARAM, RET_FAIL_EXEC
import sensor_list_pb2
import event_list_pb2


# 日志
LOG = logging.getLogger('driver_zmq')
# 驱动版本号
driver_version = 'v1.0'
# 驱动名称
driver_name = 'driver_zmq.py'
# 驱动描述
driver_desc = 'zmq驱动程序v1.0版本（20190917)'


# zmq初始化环境
context = zmq.Context()


class DataDriver:
    # 订阅配置
    class SubCfg:
        def __init__(self, host, port, topic):
            # 客户端IP地址
            self.host = host
            # 客户端端口号
            self.port = port
            # 客户端订阅主题
            self.topic = topic

    def __init__(self):
        # 订阅socket
        self.__sub_socket = context.socket(zmq.SUB)
        # 发布socket
        self.__pub_socket = context.socket(zmq.PUB)
        # 回调函数
        self.__call_back = None
        # 采集线程运行标识
        self.__running = False
        # 本地IP地址
        self.__host = ''
        # 本地端口号
        self.__port = 0
        # 数据模式：sensor(传感类数据)；event（事件类数据）
        self.__model = ''
        # 订阅数组
        self.__sub_topics_dict = {}
        # 组装数据函数字典
        self.__package = {'sensor': self.__package_sensor_data, 'event': self.__package_event_data}
        # 解析数据函数字典
        self.__un_package = {'sensor': self.__un_package_sensor_data, 'event': self.__un_package_event_data}

    # 获取版本信息
    def get_version(self):
        return json.dumps({'code': 0, 'data': {'version': driver_version, 'name': driver_name, 'desc': driver_desc}})

    # 设置基础配置
    def set_basic_config(self, cfg_name, cfg_msg):
        # 解析配置
        try:
            cfg = json.loads(cfg_msg, encoding='utf-8')
            self.__host = cfg['host']
            self.__port = cfg['port']
            self.__model = cfg['model']
        except Exception as err:
            LOG.error('设置基础配置失败:%s' % (err,))
            return RET_FAIL_PARAM
        # 校验配置
        if self.__model not in self.__package.keys():
            LOG.error('数据模式设置错误:%s' % (self.__model,))
            return RET_FAIL_EXEC
        # 设置基础配置成功
        LOG.info('设置基础配置成功')
        return RET_OK

    # 设置运行时配置
    def set_run_config(self, cfg_name, cfg_msg):
        try:
            # 临时变量，保存订阅主题
            cfg_sub_dict = {}
            # 配置所有的订阅信息
            cfg_list = json.loads(cfg_msg, encoding='utf-8')
            for cfg in cfg_list:
                host, port, topic = cfg['host'], cfg['port'], cfg['topic']
                # 组建订阅主题
                sub_key = self.__make_topic(host, port, topic)
                # 添加到临时订阅列表
                cfg_sub_dict[sub_key] = self.SubCfg(host, port, sub_key)
        except Exception as err:
            LOG.error('设置运行配置失败:%s' % (str(err),))
            return RET_FAIL_PARAM
        # 动态更新订阅配置
        if not self.__alter_topic(cfg_sub_dict):
            return RET_FAIL_EXEC
        # 设置运行配置成功
        LOG.info('设置运行配置成功')
        return RET_OK

    # 初始化
    def init(self):
        try:
            # 创建zmq服务端
            self.__pub_socket.bind('tcp://%s:%d' % (self.__host, self.__port))
            # 创建订阅线程
            thread_sub = threading.Thread(target=self.__run)
            thread_sub.start()
            LOG.info('创建zmq服务端成功：%s:%d' % (self.__host, self.__port))
        except Exception as err:
            LOG.error('创建zmq服务端失败: %s' % (str(err),))
            return RET_FAIL_EXEC
        # 初始化成功
        LOG.info('驱动初始化成功。创建zmq服务端：%s:%d' % (self.__host, self.__port))
        return RET_OK

    # 设置回调函数
    def set_fun(self, fun):
        self.__call_back = fun
        # 设置回调函数成功
        LOG.info('设置回调函数成功')
        return RET_OK

    # 写入数据（对外发布数据）
    def write_data(self, data_info, data_list):
        # 解析data_info里的topic
        info = json.loads(data_info, encoding='utf-8')
        # 组建发送标识topic
        topic = self.__make_topic(self.__host, self.__port, info['topic'])
        try:
            # 组装数据
            message = self.__package_data(data_list)
        except Exception as err:
            LOG.error('组装数据:%s' % (str(err),))
            return RET_FAIL_EXEC
        try:
            # 发送数据
            self.__pub_socket.send(topic.encode('utf-8') + message)
        except Exception as err:
            LOG.error('发布数据失败:%s' % (str(err),))
            return RET_FAIL_EXEC
        # 写入数据成功
        LOG.debug('%s主题发布%d条数据' % (str(topic), len(data_list)))
        return RET_OK

    # 启动运行
    def run(self):
        self.__running = True
        # 启动运行成功
        LOG.info('驱动启动成功')
        return RET_OK

    # 停止运行
    def stop(self):
        self.__running = False
        # 停止采集成功
        LOG.info('驱动停止成功')
        return RET_OK

    # 获取状态
    def get_state(self):
        if self.__running:
            state = 0x00000000
        else:
            state = 0x00010000
        return json.dumps({'code': 0, 'data': state})

    # ------------------------------内部实现--------------------------------

    # 组建topic（host:port:topic^）
    def __make_topic(self, host, port, topic):
        result = ':'.join((host, str(port), topic)) + '^'
        return result

    # 建立zmq订阅连接，设置订阅主题
    def __connect(self, host, port, topic):
        self.__sub_socket.connect('tcp://%s:%d' % (host, port))
        self.__sub_socket.setsockopt(zmq.SUBSCRIBE, topic.encode())

    # 删除订阅主题
    def __dis_connect(self, topic):
        self.__sub_socket.setsockopt(zmq.UNSUBSCRIBE, topic.encode())

    # 动态更新主题
    def __alter_topic(self, cfg_sub_dict):
        try:
            for sub_key, sub_info in cfg_sub_dict.items():
                # 检查是否为新增订阅
                if sub_key not in self.__sub_topics_dict:
                    # 新增订阅
                    self.__connect(sub_info.host, sub_info.port, sub_info.topic)
                    # 设置订阅主题
                    self.__sub_topics_dict[sub_key] = sub_info
                    LOG.info('新增订阅主题:%s' % (sub_info.topic,))
            # 差值计算，查找要删除的订阅
            del_keys = list(set(self.__sub_topics_dict.keys()).difference(set(cfg_sub_dict.keys())))
            for key in del_keys:
                # 退订
                self.__dis_connect(cfg_sub_dict[key].topic)
                # 删除对应主题
                self.__sub_topics_dict.pop(key)
                LOG.info('删除订阅主题:%s' % (cfg_sub_dict[key].topic,))
            return True
        except Exception as err:
            LOG.error('更新主题配置失败: %s' % (str(err),))
            return False

    # 主运行函数
    def __run(self):
        while True:
            if self.__running:
                # 接收数据
                message = self.__sub_socket.recv()
                topic, message = message.split(b'^')
                # 解析数据
                data_list = self.__parse_data(message)
                # 组装data_info
                data_info = json.dumps({'topic': topic.decode()})
                LOG.debug('接收数据:%s, 长度:%d' % (str(data_info), len(data_list)))
                # 执行回调函数，反馈采集结果
                self.__call_back(data_info, data_list)
            else:
                time.sleep(1)

    # 组装数据
    def __package_data(self, data_list):
        return self.__package[self.__model](data_list)

    # 解析数据
    def __parse_data(self, message):
        return self.__un_package[self.__model](message)

    # 组装传感类数据
    def __package_sensor_data(self, data_list):
        sensor_list = sensor_list_pb2.sensor_list()
        for data in data_list:
            sensor = sensor_list.sensors.add()
            sensor.tag = data.tag
            sensor.value_double = data.value_double
            sensor.value_int64 = data.value_int64
            sensor.value_string = data.value_string
            sensor.value_bytes = data.value_bytes
            sensor.data_type = data.data_type
            sensor.quality = data.quality
            sensor.timestamp = data.timestamp
        return sensor_list.SerializeToString()

    # 解析传感类数据
    def __un_package_sensor_data(self, msg_data):
        data_list = []
        sensors_list = sensor_list_pb2.sensor_list()
        sensors_list.ParseFromString(msg_data)
        for sensor in sensors_list.sensors:
            data = Data(sensor.tag, sensor.data_type, sensor.quality, sensor.timestamp,
                        value_double=sensor.value_double,
                        value_int64=sensor.value_int64,
                        value_string=sensor.value_string,
                        value_bytes=sensor.value_bytes)
            data_list.append(data)
        return data_list

    # 组装事件类数据
    def __package_event_data(self, data_list):
        event_list = event_list_pb2.event_list()
        for data in data_list:
            event = event_list.sensors.add()
            event.source = data.source
            event.event_type = data.event_type
            event.level = data.level
            event.keyword = data.keyword
            event.content = data.content
            event.timestamp = data.timestamp
        return event_list.SerializeToString()

    # 解析事件类数据
    def __un_package_event_data(self, msg_data):
        data_list = []
        event_list = event_list_pb2.event_list()
        event_list.ParseFromString(msg_data)
        for event in event_list.events:
            data = Event(event.source, event.event_type, event.level, event.keyword, event.content, event.timestamp)
            data_list.append(data)
        return data_list

