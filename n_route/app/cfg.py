#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: jiaruya
@file: cfg.py
@time: 2019/09/08
@desc:
"""
import json
from share import LOG
from share import load_config
from share import Data_Mode_Dict, Data_dir_Dict


# 基础配置
class BasicInfo:
    def __init__(self, server_name, server_version, server_desc, local_ip, local_port, sequence_max, discrete_max, domain):
        # 服务名称
        self.server_name = server_name
        # 服务版本号
        self.server_version = server_version
        # 服务描述
        self.server_desc = server_desc
        # 本机ip
        self.local_ip = local_ip
        # 本机端口号
        self.local_port = local_port
        # 数组最大缓存个数
        self.sequence_max = sequence_max
        # 队列最大缓存个数
        self.discrete_max = discrete_max
        # 数据域
        self.domain = domain


# 驱动配置
class DriverInfo:
    def __init__(self, driver_name, lib_name, lib_type, basic_config, run_config, direction, sub_type, pub_type):
        # 驱动名称
        self.driver_name = driver_name
        # 驱动库名称
        self.lib_name = lib_name
        # 驱动编程语言
        self.lib_type = lib_type
        # 驱动基础配置文件
        self.basic_config = basic_config
        # 驱动运行配置文件
        self.run_config = run_config
        # 数据流向（采集为下：down, 发布为上：up）
        self.direction = direction
        # 订阅数据类型（seq_sensor：连续传感类数据；dis_sensor:离散传感类数据；dis_event:离散事件类数据）
        self.sub_type = sub_type
        # 发布数据类型（seq_sensor：连续传感类数据；dis_sensor:离散传感类数据；dis_event:离散事件类数据）
        self.pub_type = pub_type
        # 驱动对象
        self.driver = None


# 主题配置
class TopicInfo:
    def __init__(self, topic, tags):
        # 发布主题
        self.topic = topic
        # 发布标签点
        self.tags = tags


# 发布泵配置
class PumpInfo:
    def __init__(self, topic, driver, cycle):
        # 发布主题
        self.topic = topic
        # 发布驱动名称
        self.driver = driver
        # 发布周期
        self.cycle = cycle


# 配置对象
class Cfg:
    def __init__(self):
        # 配置对象字典
        self.__cfg_msg = {}

    # 加载配置
    def load_cfg(self, cfg_name):
        try:
            self.__cfg_msg = load_config(cfg_name)
        except Exception as err:
            LOG.error('加载%s配置文件失败:%s' % (cfg_name, err))
            return False
        LOG.info('加载%s配置成功' % cfg_name)

    # 加载基础配置
    def get_basic_cfg(self):
        basic = BasicInfo(self.__cfg_msg['server_name'],
                          self.__cfg_msg['server_version'],
                          self.__cfg_msg['server_desc'],
                          self.__cfg_msg['local_ip'],
                          self.__cfg_msg['local_port'],
                          self.__cfg_msg['sequence_max'],
                          self.__cfg_msg['discrete_max'],
                          self.__cfg_msg['domain'])
        return basic

    # 加载驱动配置
    def get_driver_cfg(self):
        drivers_dict = {}
        if 'driver' not in self.__cfg_msg:
            return drivers_dict
        driver_cfg = self.__cfg_msg['driver']
        for cfg in driver_cfg:
            driver = DriverInfo(cfg['driver_name'],
                                cfg['lib_name'],
                                cfg['lib_type'],
                                cfg['basic_config'],
                                cfg['run_config'],
                                cfg['direction'],
                                cfg['sub_type'],
                                cfg['pub_type'])
            drivers_dict[driver.driver_name] = driver
        # 关键配置校验
        for driver in drivers_dict.values():
            if driver.direction not in Data_dir_Dict:
                LOG.error('驱动配置错误: %s: %s' % (driver.driver_name, driver.direction))
            if driver.sub_type and driver.sub_type not in Data_Mode_Dict:
                LOG.error('驱动配置错误: %s: %s' % (driver.driver_name, driver.sub_type))
            if driver.pub_type and driver.pub_type not in Data_Mode_Dict:
                LOG.error('驱动配置错误: %s: %s' % (driver.driver_name, driver.pub_type))
        return drivers_dict

    # 加载主题配置
    def get_topic_cfg(self):
        topics_dict = {}
        if 'topic' not in self.__cfg_msg:
            return topics_dict
        topic_cfg = self.__cfg_msg['topic']
        for cfg in topic_cfg:
            topic = TopicInfo(cfg['topic'], cfg['tags'])
            topics_dict[topic.topic] = topic
        return topics_dict

    # 加载发布配置
    def get_pump_cfg(self):
        pumps = []
        if 'pump' not in self.__cfg_msg:
            return pumps
        pump_cfg = self.__cfg_msg['pump']
        for cfg in pump_cfg:
            pump = PumpInfo(cfg['topic'], cfg['driver'], cfg['cycle'])
            pumps.append(pump)
        for pump in pumps:
            if pump.topic not in self.get_topic_cfg():
                LOG.error('发布泵配置错误: %s: %s' % (pump.driver, pump.topic))
            if pump.driver not in self.get_driver_cfg():
                LOG.error('发布泵配置错误: %s: %s' % (pump.driver, pump.topic))
        return pumps


if __name__ == "__main__":
    print("hello world")
