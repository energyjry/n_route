#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: jiaruya
@file: driver_websocket.py
@time: 2019/09/18
@desc:
"""

import json
import threading
import logging
from websocket_server import WebsocketServer
from share import sensor2dict, event2dict
from share import RET_OK, RET_FAIL_PARAM, RET_FAIL_EXEC

# 日志
LOG = logging.getLogger('driver_websocket')
# 驱动版本号
driver_version = 'v1.0'
# 驱动名称
driver_name = 'driver_websocket.py'
# 驱动描述
driver_desc = 'websocket驱动程序v1.0版本（20190918)'


class DataDriver:
    # 客户端信息
    class ClientInfo:
        def __init__(self, client, address):
            # 客户端对象
            self.client = client
            # 客户端地址
            self.address = address
            # 客户端订阅主题
            self.topic = ''

    def __init__(self):
        # 本地ip地址
        self.__host = ''
        # 本地端口号
        self.__port = 0
        # 数据模式：sensor(传感类数据)；event（事件类数据）
        self.__model = ''
        # websocket服务器对象
        self.__web_server = None
        # 客户端字典
        self.__client_dict = {}
        # 组装数据函数字典
        self.__package = {'sensor': self.__package_sensor_data, 'event': self.__package_event_data}

    # 获取版本信息
    def get_version(self):
        return json.dumps({'code': 0, 'data': {'version': driver_version, 'name': driver_name, 'desc': driver_desc}})

    # 设置基础配置
    def set_basic_config(self, cfg_name, cfg_msg):
        # 解析配置
        try:
            cfg = json.loads(cfg_msg, encoding='utf-8')
            self.__host, self.__port, self.__model = cfg['host'], cfg['port'], cfg['model']
        except Exception as err:
            LOG.error('解析基础配置失败:%s' % (str(err),))
            return RET_FAIL_PARAM
        # 校验配置
        if self.__model not in ('sensor', 'event'):
            LOG.error('校验基础配置失败:%s' % self.__model)
            return RET_FAIL_PARAM
        LOG.info('设置基础配置成功')
        return RET_OK

    # 设置运行时配置
    def set_run_config(self, cfg_name, cfg_msg):
        # 无运行时配置，直接返回
        return RET_OK

    # 初始化
    def init(self):
        try:
            # 启动WebSocket服务
            self.__web_server = WebsocketServer(self.__port, self.__host)
            # 注册回调函数
            self.__web_server.set_fn_new_client(self.__new_client)
            self.__web_server.set_fn_client_left(self.__client_left)
            self.__web_server.set_fn_message_received(self.__msg_receive)
            # 创建WebSocket服务线程
            t1 = threading.Thread(target=self.__web_server.run_forever)
            t1.start()
        except Exception as err:
            LOG.error('初始化失败:%s' % (str(err),))
            return RET_FAIL_EXEC
        LOG.info('驱动初始化成功。创建websocket服务端：%s:%d' % (self.__host, self.__port))
        return RET_OK

    # 设置回调函数
    def set_fun(self, fun):
        # 无需回调，直接返回
        return RET_OK

    # 写入数据（对外发布数据）
    def write_data(self, data_info, data_list):
        try:
            # 解析data_info里的topic
            data_info = json.loads(data_info, encoding='utf-8')
            topic = data_info['topic']
            for info in self.__client_dict.values():
                if topic == info.topic:
                    self.__web_server.send_message(info.client, self.__package_data(data_list))
        except Exception as err:
            LOG.error('发布数据失败:%s' % str(err))
            return RET_FAIL_EXEC
        return RET_OK

    # 启动运行
    def run(self):
        # 无需启动，直接返回
        return RET_OK

    # 停止运行
    def stop(self):
        # 无需停止，直接返回
        return RET_OK

    # 获取状态
    def get_state(self):
        state = 0x00000000
        return json.dumps({'code': 0, 'data': state})

    # ---------------------------------------内部实现---------------------------------------------
    # 新客户端连接
    def __new_client(self, client, server):
        if client:
            LOG.info("新的页面客户端连接： %d, %s" % (client['id'], str(client['address'])))
            self.__client_dict[client['id']] = self.ClientInfo(client, client['address'])

    # 接收到页面请求
    def __msg_receive(self, client, server, message):
        if client:
            LOG.info("接收到页面%d请求:%s..." % (client['id'], message[:100]))
            try:
                msg_dict = json.loads(message)
                topic = msg_dict['topic']
                self.__client_dict[client['id']].topic = topic
            except Exception:
                server.send_message(client, 'pong')

    # 页面离开
    def __client_left(self, client, server):
        if client:
            LOG.info('页面断开连接：%d, %s' % (client['id'], str(client['address'])))
            if client['id'] in self.__client_dict:
                self.__client_dict.pop(client['id'])

    # 组装数据
    def __package_data(self, data_list):
        return self.__package[self.__model](data_list)

    # 组装传感类数据
    def __package_sensor_data(self, data_list):
        datas = []
        for data in data_list:
            datas.append(sensor2dict(data))
        return json.dumps(datas)

    def __package_event_data(self, data_list):
        datas = []
        for data in data_list:
            datas.append(event2dict(data))
        return json.dumps(datas)

if __name__ == "__main__":
    print("hello world")
