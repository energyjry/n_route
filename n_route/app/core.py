#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: jiaruya
@file: core.py
@time: 2019/09/09
@desc:
"""
import json
import time
from pump import PubSeq, PubDis
from share import Data
from store import Sequence, Discrete
from cfg import Cfg
from share import LOG, DEBUG
from share import load_python, load_csharp, load_config, check_api_result


class Core:
    class TopicDataInfo:
        def __init__(self, topic, data_count, last_time):
            self.topic = topic
            self.data_count = data_count
            self.last_time = last_time

    def __init__(self):
        # 配置
        self.m_cfg = Cfg()
        # 基础配置
        self.m_cfg.load_cfg('basic.json')
        self.basic_info = self.m_cfg.get_basic_cfg()
        # 下行连续数据区
        self.m_store_seq_down = Sequence(self.basic_info.sequence_max)
        # 下行离散数据区
        self.m_store_dis_down = Discrete(self.basic_info.discrete_max)
        # 上行连续数据区
        self.m_store_seq_up = Sequence(self.basic_info.sequence_max)
        # 上行离散数据区
        self.m_store_dis_up = Discrete(self.basic_info.discrete_max)
        # 上行连续发布泵（关联下行连续数据区）
        self.m_pump_seq_up = PubSeq(self.m_store_seq_down)
        # 上行离散发布泵（关联下行离散数据区）
        self.m_pump_dis_up = PubDis(self.m_store_dis_down)
        # 下行连续发布泵（关联上行连续数据区）
        self.m_pump_seq_down = PubSeq(self.m_store_seq_up)
        # 下行离散发布泵（关联上行离散数据区）
        self.m_pump_dis_down = PubDis(self.m_store_dis_up)
        # 驱动字典
        self.m_driver_dict = {}
        # 主题字典
        self.m_topic_dict = {}
        # 主题数据字典
        self.m_topic_data_dict = {}

    # 加载配置
    def load_cfg(self, file_name):
        self.m_cfg.load_cfg(file_name)

    # 加载驱动
    def load_driver(self):
        self.m_driver_dict = self.m_cfg.get_driver_cfg()
        for driver_info in self.m_driver_dict.values():
            # 加载驱动对象
            if driver_info.lib_type == 'python':
                # python版本的驱动
                driver = load_python(driver_info.lib_name)
            elif driver_info.lib_type == 'csharp':
                # C#版本的驱动
                driver = load_csharp(driver_info.driver_name)
            else:
                # 其它语言版本的驱动暂不支持（待扩充）
                driver = None
                LOG.error('不支持的库类型:%s' % (driver_info.lib_type,))
            if driver:
                driver_info.driver = driver
                LOG.info('加载驱动成功:%s' % (driver_info.driver_name,))
            else:
                LOG.error('加载驱动失败:%s' % (driver_info.driver_name,))

    # 加载主题
    def load_topic(self):
        self.m_topic_dict = self.m_cfg.get_topic_cfg()

    # 初始化驱动
    def init_driver(self):
        # 遍历驱动字典
        for driver_info in self.m_driver_dict.values():
            if not driver_info.driver:
                LOG.error('驱动%s未加载' % (driver_info.driver_name,))
                continue
            # 1.设置驱动基础配置
            if not self.__driver_set_basic_config(driver_info):
                continue
            # 2.设置驱动运行配置
            if not self.__driver_set_run_config(driver_info):
                continue
            # 3.驱动初始化操作
            ret = driver_info.driver.init()
            if check_api_result(ret):
                LOG.info('驱动 %s 初始化成功' % driver_info.driver_name)
            else:
                LOG.error('驱动 %s 初始化失败' % driver_info.driver_name)
                continue
            # 4.设置回调函数
            if not self.__driver_set_call_back(driver_info):
                continue
            # 5.启动驱动
            ret = driver_info.driver.run()
            if check_api_result(ret):
                LOG.info('启动 %s 成功' % driver_info.driver_name)
            else:
                LOG.error('启动 %s 失败' % driver_info.driver_name)
                continue

    # 加载发布泵
    def load_pump(self):
        pumbs_cfg = self.m_cfg.get_pump_cfg()
        # 遍历发布泵配置
        for pump_info in pumbs_cfg:
            # 主题配置信息
            if pump_info.topic not in self.m_topic_dict:
                LOG.error('%s 主题未配置' % (pump_info.topic,))
                continue
            topic_info = self.m_topic_dict[pump_info.topic]
            # 驱动配置信息
            if pump_info.driver not in self.m_driver_dict:
                LOG.error('%s 驱动未配置' % (pump_info.driver,))
                continue
            driver_info = self.m_driver_dict[pump_info.driver]
            # 关联发布泵:pump,driver,topic,cycle
            if self.__pump_set_config(driver_info, topic_info, pump_info.cycle):
                LOG.info('关联发布泵成功:%s:%s:%d' % (driver_info.driver_name, topic_info.topic, pump_info.cycle))
            else:
                continue

    # 设置运行时配置
    def set_run_config(self, cfg_name, cfg_msg):
        for driver_info in self.m_driver_dict.values():
            # 查找到对应的驱动
            if driver_info.config['cfg'] == cfg_name:
                # 设置驱动运行时配置
                return self.__driver_set_run_config(driver_info)
        return False

    # 主进行函数
    def my_main(self):
        # 加载驱动
        self.load_driver()
        # 加载主题
        self.load_topic()
        # 初始化驱动
        self.init_driver()
        # 加载发布泵
        self.load_pump()
        if DEBUG:
            self.__test()

    # -----------------内部函数---------------------

    # 写入下行连续数据区
    def __write_data_seq_sensor_down(self, data_info, data_list):
        # 将数据写入到数据缓存区
        for data in data_list:
            self.m_store_seq_down.put_data(data.tag, data)
        # 保存数据状态信息
        self.__save_data_info(data_info, len(data_list))

    # 写入下行连续数据区
    def __write_data_seq_event_down(self, data_info, data_list):
        # 将数据写入到数据缓存区
        for data in data_list:
            key = data.source + data.keyword
            self.m_store_seq_down.put_data(key, data)
        # 保存数据状态信息
        self.__save_data_info(data_info, len(data_list))

    # 写入下行离散数据区
    def __write_data_dis_down(self, data_info, data_list):
        # 将数据写入到数据缓存区
        for data in data_list:
            self.m_store_dis_down.put_data(data)
        # 保存数据状态信息
        self.__save_data_info(data_info, len(data_list))

    # 写入上行连续数据区
    def __write_data_seq_sensor_up(self, data_info, data_list):
        # 将数据写入到数据缓存区
        for data in data_list:
            self.m_store_seq_up.put_data(data.tag, data)
        # 保存数据状态信息
        self.__save_data_info(data_info, len(data_list))

    # 写入上行连续数据区
    def __write_data_seq_event_up(self, data_info, data_list):
        # 将数据写入到数据缓存区
        for data in data_list:
            key = data.source + data.keyword
            self.m_store_seq_up.put_data(key, data)
        # 保存数据状态信息
        self.__save_data_info(data_info, len(data_list))

    # 写入上行离散数据区
    def __write_data_dis_up(self, data_info, data_list):
        # 将数据写入到数据缓存区
        for data in data_list:
            self.m_store_dis_up.put_data(data)
        # 保存数据状态信息
        self.__save_data_info(data_info, len(data_list))

    # 保存主题数据状态
    def __save_data_info(self, data_info, data_len):
        data_info = json.loads(data_info)
        if 'topic' in data_info and len(data_info['topic']) > 0:
            # 保存当前主题对应的数据信息（主题、个数、最后更新时间）
            self.m_topic_data_dict[data_info['topic']] = self.TopicDataInfo(data_info['topic'], data_len, int(time.time()))
        else:
            # 默认主题（主题、个数、最后更新时间）
            topic = 'all'
            self.m_topic_data_dict[topic] = self.TopicDataInfo(topic, data_len, int(time.time()))

    # 驱动设置基础配置
    def __driver_set_basic_config(self, driver_info):
        if driver_info.basic_config:
            ret = driver_info.driver.set_basic_config(driver_info.basic_config,
                                                      json.dumps(load_config(driver_info.basic_config)))
            if not check_api_result(ret):
                LOG.error('驱动 %s 设置基础配置失败' % driver_info.driver_name)
                return False
            LOG.info('驱动 %s 设置基础配置成功' % driver_info.driver_name)
        return True

    # 驱动设置运行配置
    def __driver_set_run_config(self, driver_info):
        if driver_info.run_config:
            ret = driver_info.driver.set_run_config(driver_info.run_config,
                                                    json.dumps(load_config(driver_info.run_config)))
            if not check_api_result(ret):
                LOG.error('驱动 %s 设置运行配置失败' % driver_info.driver_name)
                return False
            LOG.info('驱动 %s 设置运行配置成功' % driver_info.driver_name)
        return True

    # 驱动设置回调函数
    def __driver_set_call_back(self, driver_info):
        if driver_info.direction == 'down':
            if driver_info.sub_type == 'seq_sensor':
                # 设置下行连续回调
                ret = driver_info.driver.set_fun(self.__write_data_seq_sensor_down)
            elif driver_info.sub_type == 'seq_event':
                # 设置下行连续回调
                ret = driver_info.driver.set_fun(self.__write_data_seq_event_down)
            elif driver_info.sub_type in ('dis_sensor', 'dis_event'):
                # 设置下行离散回调
                ret = driver_info.driver.set_fun(self.__write_data_dis_down)
            else:
                if driver_info.sub_type is not None:
                    LOG.error('驱动%s采集方式配置错误:%s' % (driver_info.driver_name, driver_info.sub_type))
                return False
        elif driver_info.direction == 'up':
            if driver_info.sub_type == 'seq_sensor':
                # 设置上行连续回调
                ret = driver_info.driver.set_fun(self.__write_data_seq_sensor_up)
            elif driver_info.sub_type == 'seq_event':
                # 设置上行连续回调
                ret = driver_info.driver.set_fun(self.__write_data_seq_event_up)
            elif driver_info.sub_type in ('dis_sensor', 'dis_event'):
                # 设置上行离散回调
                ret = driver_info.driver.set_fun(self.__write_data_dis_up)
            else:
                if driver_info.sub_type is not None:
                    LOG.error('驱动%s采集方式配置错误:%s' % (driver_info.driver_name, driver_info.sub_type))
                return False
        else:
            LOG.error('驱动%s数据流向配置错误:%s' % (driver_info.driver_name, driver_info.direction))
            return False
        if not check_api_result(ret):
            LOG.error('设置 %s 回调失败' % driver_info.driver_name)
            return False
        LOG.info('设置 %s 回调成功' % driver_info.driver_name)
        return True

    # 关联pump,topic,driver
    def __pump_set_topic_driver(self, pump, topic_info, driver_info, cycle):
        pump.set_topic_info(topic_info.topic, topic_info.tags)
        pump.set_driver_info(driver_info.driver_name, driver_info.driver, topic_info.topic, cycle)

    # 发布泵配置
    def __pump_set_config(self, driver_info, topic_info, cycle):
        if driver_info.direction == 'down':
            if driver_info.pub_type in ('seq_sensor', 'seq_event'):
                # 设置下行连续泵
                self.__pump_set_topic_driver(self.m_pump_seq_down, topic_info, driver_info, cycle)
            elif driver_info.pub_type in ('dis_sensor', 'dis_event'):
                # 设置下行离散泵
                self.__pump_set_topic_driver(self.m_pump_dis_down, topic_info, driver_info, cycle)
            else:
                LOG.error('驱动 %s 发布方式配置错误：%s' % (driver_info.driver_name, driver_info.pub_type))
                return False
        elif driver_info.direction == 'up':
            if driver_info.pub_type in ('seq_sensor', 'seq_event'):
                # 设置上行连续泵
                self.__pump_set_topic_driver(self.m_pump_seq_up, topic_info, driver_info, cycle)
            elif driver_info.pub_type in ('dis_sensor', 'dis_event'):
                # 设置上行离散泵
                self.__pump_set_topic_driver(self.m_pump_dis_up, topic_info, driver_info, cycle)
            else:
                LOG.error('驱动 %s 发布方式配置错误：%s' % (driver_info.driver_name, driver_info.pub_type))
                return False
        else:
            LOG.error('驱动 %s 数据流向配置错误:%s ' % (driver_info.driver_name, driver_info.direction))
            return False
        return True

    # 测试
    def __test(self):
        data = Data('tag1', 0, 192, int(time.time()), value_double=1.1)
        self.m_store_seq_down.put_data(data.tag, data)
        data = Data('tag2', 1, 192, int(time.time()), value_int64=8)
        self.m_store_seq_down.put_data(data.tag, data)
        data = Data('tag3', 2, 192, int(time.time()), value_string='hello')
        self.m_store_seq_down.put_data(data.tag, data)
        data = Data('tag4', 3, 192, int(time.time()), value_bytes=b'fuck')
        self.m_store_seq_down.put_data(data.tag, data)
        data = Data('tag10', 2, 192, int(time.time()), value_string='hello')
        self.m_store_dis_up.put_data(data)


if __name__ == "__main__":
    m_core = Core()
    m_core.my_main()
    while True:
        time.sleep(1)
