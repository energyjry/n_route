#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: jiaruya
@file: pump.py
@time: 2019/09/05
@desc:
"""
import json
import threading
import time
from share import DEBUG


# 发布连续数据
class PubSeq:
    # 驱动配置信息
    class __DriverInfo:
        def __init__(self, driver, topic, cycle):
            # 发布驱动
            self.driver = driver
            # 订阅主题
            self.topic = topic
            # 发布周期
            self.cycle = cycle
            # 发布计数器
            self.count = 0

    # 主题配置信息
    class __TopicInfo:
        # 主题功能待完善扩充
        def __init__(self, topic, tags):
            # 订阅主题
            self.topic = topic
            # 标签点集合
            self.tags = tags.split(',')
            # 索引位置集合
            self.indexs = []
            # 索引位置查找标识
            self.index_flag = False

    def __init__(self, data_store):
        # 数据源
        self.__data_store = data_store
        # 主题配置字典（供读取、筛选数据使用）
        self.__topic_dict = {}
        # 驱动配置字典（供发布数据使用）
        self.__driver_dict = {}
        # 数据域（接入节点发布数据时，需要添加数据域）
        self.__domain = ''
        # 创建发布数据线程
        self.__thread_pub = threading.Thread(target=self.__fun)
        # 启动发布数据线程
        self.__thread_pub.start()

    # 设置数据域
    def set_data_domain(self, domain):
        self.__domain = domain

    # 设置主题
    def set_topic_info(self, topic, tags):
        self.__topic_dict[topic] = self.__TopicInfo(topic, tags)

    # 设置驱动
    def set_driver_info(self, driver_name, driver, topic, cycle):
        # 驱动名称+发布主题
        key = driver_name + topic
        self.__driver_dict[key] = self.__DriverInfo(driver, topic, cycle)

    # 根据主题获取数据（待扩充完善）
    def __get_data_by_topic(self, topic_info):
        if topic_info.topic == 'all':
            # all主题，获取全部数据
            data_list = self.__data_store.get_data_all()
        else:
            # 其它主题,查找索引位置集合
            if not topic_info.index_flag:
                # 查询索引位置
                topic_info.indexs.clear()
                topic_info.index_flag = True
                for tag in topic_info.tags:
                    # 根据tag查找index
                    index = self.__data_store.get_index(tag)
                    if index == -1:
                        # 查找索引失败
                        topic_info.index_flag = False
                    topic_info.indexs.append(index)
            # 根据索引位置查找数据
            data_list = []
            for index in topic_info.indexs:
                data = self.__data_store.get_data(index)
                if data:
                    # 读取到数据后添加到数组
                    data_list.append(data)
        if len(self.__domain) > 0:
            # 如果存在数据域
            for data in data_list:
                # 对测点tag添加数据域
                data.tag = self.__domain + ':' + data.tag
        return data_list

    # 发布数据函数
    def __fun(self):
        while True:
            b_time = time.time()
            # 遍历所有驱动
            for info in self.__driver_dict.values():
                # 驱动达到发送周期
                if info.count == 0:
                    # 查找驱动对应的主题配置
                    topic_info = self.__topic_dict[info.topic]
                    # 根据主题查找数据
                    data_list = self.__get_data_by_topic(topic_info)
                    # 驱动发送数据
                    info.driver.write_data(json.dumps({'topic': info.topic}), data_list)
                # 计数器加1
                info.count += 1
                if info.count >= info.cycle:
                    # 计数器达到发送周期时，计数器清零。触发发送数据
                    info.count = 0
            # 每个轮询周期休眠1秒
            e_time = time.time()
            used_time = e_time - b_time
            if used_time < 1:
                time.sleep(1 - used_time)


# 发布离散数据
class PubDis:
    # 驱动配置信息
    class __DriverInfo:
        def __init__(self, driver, topic):
            # 发布驱动
            self.driver = driver
            # 主题
            self.topic = topic

    # 主题配置信息
    class __TopicInfo:
        def __init__(self, topic, tags):
            # 主题
            self.topic = topic
            # 测点
            self.tags = tags.split(',')

    def __init__(self, data_store):
        # 数据源
        self.__data_store = data_store
        # 主题配置字典（供读取、筛选数据使用）
        self.__topic_dict = {}
        # 驱动配置字典（供发布数据使用）
        self.__driver_dict = {}
        # 数据域（接入节点发布数据时，需要添加数据域）
        self.__domain = ''
        # 创建发布数据线程
        self.__thread_pub = threading.Thread(target=self.__fun)
        # 启动发布数据线程
        self.__thread_pub.start()

    # 设置数据域
    def set_data_domain(self, domain):
        self.__domain = domain

    # 设置主题
    def set_topic_info(self, topic, tags=''):
        self.__topic_dict[topic] = self.__TopicInfo(topic, tags)

    # 设置驱动
    def set_driver_info(self, driver_name, driver, topic, cycle=0):
        # 驱动名称+发布主题
        key = driver_name + topic
        self.__driver_dict[key] = self.__DriverInfo(driver, topic)

    # 根据主题筛选数据（待扩充完善）
    def __get_data_by_topic(self, data, topic_info):
        if topic_info.topic == 'control':
            # all主题，返回全部数据
            return data
        else:
            # 目前对离散数据值过滤，只实现对传感类数据的标签点过滤
            try:
                if data.tag in topic_info.tags:
                    return data
                else:
                    return None
            except Exception:
                return None

    # 发布数据函数
    def __fun(self):
        while True:
            # 取出数据
            data = self.__data_store.get_data()
            if data:
                for info in self.__driver_dict.values():
                    # 查找驱动对应的主题配置
                    topic_info = self.__topic_dict[info.topic]
                    # 根据主题对数据进行过滤
                    out_data = self.__get_data_by_topic(data, topic_info)
                    if out_data:
                        if len(self.__domain) > 0:
                            data.tag = self.__domain + '.' + data.tag
                        # 驱动发送数据
                        info.driver.write_data(json.dumps({'topic': info.topic}), [out_data])
            # 离散数据读数为阻塞方式
            if DEBUG:
                time.sleep(2)
