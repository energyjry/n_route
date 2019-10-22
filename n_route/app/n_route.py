#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: jiaruya
@file: n_route.py
@time: 2019/09/08
@desc:
"""
import json
from flask import Flask
from flask_restplus import Api
from core import Core
from share import CFG_PATH, LOG, Data
from share import sensor2dict
from interface import IVersion, IConfig, IReadTopic, IReadTag, IWriteTag, IFlushData, IState
from interface import REST_VERSION, REST_CONFIG, REST_READ_TOPIC, REST_READ_TAG, REST_FLUSH_DATA, REST_STATE


class Version(IVersion):
    def get_version(self):
        try:
            return True, {'serv_name': basic_info.server_name, 'serv_version': basic_info.server_version,
                          'serv_desc': basic_info.server_desc}
        except Exception as err:
            return False, str(err)


class Config(IConfig):
    def set_config(self, cfg_name, cfg_msg):
        file_name = CFG_PATH + cfg_name + '.json'
        if type(cfg_msg) == dict or type(cfg_msg) == list:
            cfg_msg = json.dumps(cfg_msg, ensure_ascii=False, indent=2)
        try:
            with open(file_name, 'w', encoding='utf-8') as f:
                f.write(cfg_msg)
            ret = '保存配置:%s:%s' % (file_name, cfg_msg)
            LOG.info(ret)
            m_core.set_run_config(file_name, cfg_msg)
            return True, cfg_msg
        except Exception as err:
            ret = '保存配置失败:%s' % (str(err),)
            LOG.error(ret)
            return False, str(err)

    def get_config(self, cfg_name):
        file_name = CFG_PATH + cfg_name + '.json'
        try:
            with open(file_name, 'r', encoding='UTF-8') as f:
                cfg_msg = json.loads(f.read())
            return True, cfg_msg
        except Exception as err:
            ret = '读取配置失败:%s' % (str(err),)
            LOG.error(ret)
            return False, str(err)


class ReadTopic(IReadTopic):
    def read_topic(self, topic):
        # 目前只支持对下行连续数据的读取
        try:
            if topic == 'all':
                # all主题
                data_list = m_core.m_store_seq_down.get_data_all()
            else:
                # 其它主题（按照tag进行过滤）
                data_list = []
                topic_info = m_core.m_cfg.get_topic_cfg()
                if topic in topic_info:
                    index_list = []
                    tags = topic_info.tags.split(',')
                    for tag in tags:
                        index_list.append(m_core.m_store_seq_down.get_index(tag))
                    for index in index_list:
                        data_list.append(m_core.m_store_seq_down.get_data(index))
            datas = []
            for data in data_list:
                datas.append(sensor2dict(data))
            return True, datas
        except Exception as err:
            return False, str(err)


class ReadTag(IReadTag):
    def read_tags(self, tags):
        # 目前只支持对下行连续数据的读取
        try:
            # 按照tag进行过滤
            data_list, index_list = [], []
            for tag in tags:
                index_list.append(m_core.m_store_seq_down.get_index(tag))
            for index in index_list:
                data = m_core.m_store_seq_down.get_data(index)
                data_list.append(sensor2dict(data))
            return True, data_list
        except Exception as err:
            return False, str(err)


class WriteTag(IWriteTag):
    def write_data(self, tag, value_double, value_int64, value_string, value_bytes, data_type, quality, timestamp):
        # 写入数据（目前只支持写入上行离散数据，即：反写控制）
        try:
            data = Data(tag, data_type, quality, timestamp, value_double=value_double, value_int64=value_int64,
                        value_string=value_string, value_bytes=value_bytes.encode())
            m_core.m_store_dis_up.put_data(data)
            return True, '写入数据成功'
        except Exception as err:
            return False, str(err)


class FlushData(IFlushData):
    def flush_data(self):
        # 清空数据区（供运维使用）
        try:
            m_core.m_store_seq_down.clear()
            m_core.m_store_dis_down.clear()
            m_core.m_store_seq_up.clear()
            m_core.m_store_dis_up.clear()
            return True, '清除数据成功'
        except Exception as err:
            return False, str(err)


class State(IState):
    def get_state(self):
        # 获取节点状态
        try:
            node_state = {}
            # 节点自身状态
            node_state['run_state'] = 0
            data_state = {}
            # 节点数据区状态（目前只支持下行数据区状态）
            data_state['seq_max'] = m_core.basic_info.sequence_max
            data_state['seq_count'] = m_core.m_store_seq_down.get_data_len()
            data_state['dis_max'] = m_core.basic_info.discrete_max
            data_state['dis_count'] = m_core.m_store_dis_down.get_data_len()
            node_state['data_state'] = data_state
            node_state['driver_state'] = []
            # 驱动状态
            for driver_info in m_core.m_driver_dict.values():
                driver_state = {}
                driver_state['driver_name'] = driver_info.driver_name
                if driver_info.driver:
                    ret = json.loads(driver_info.driver.get_state())
                    driver_state['driver_state'] = ret['data']
                else:
                    driver_state['driver_state'] = -1
                node_state['driver_state'].append(driver_state)
            node_state['topic_state'] = []
            # 主题数据状态
            data_topic_state = m_core.m_topic_data_dict
            for topic_info in data_topic_state.values():
                topic_state = {}
                topic_state['topic'] = topic_info.topic
                topic_state['count'] = topic_info.data_count
                topic_state['time'] = topic_info.last_time
                node_state['topic_state'].append(topic_state)
            return True, node_state
        except Exception as err:
            return False, str(err)

if __name__ == "__main__":
    m_core = Core()
    m_core.my_main()
    basic_info = m_core.basic_info
    app = Flask(__name__)
    api = Api(app, version=basic_info.server_version, title=basic_info.server_name, description=basic_info.server_desc)
    api.add_resource(Version, '/' + basic_info.server_name + REST_VERSION)
    api.add_resource(Config, '/' + basic_info.server_name + '/' + basic_info.server_version + REST_CONFIG)
    api.add_resource(ReadTopic, '/' + basic_info.server_name + '/' + basic_info.server_version + REST_READ_TOPIC)
    api.add_resource(ReadTag, '/' + basic_info.server_name + '/' + basic_info.server_version + REST_READ_TAG)
    api.add_resource(WriteTag, '/' + basic_info.server_name + '/' + basic_info.server_version + REST_READ_TAG)
    api.add_resource(FlushData, '/' + basic_info.server_name + '/' + basic_info.server_version + REST_FLUSH_DATA)
    api.add_resource(State, '/' + basic_info.server_name + '/' + basic_info.server_version + REST_STATE)
    app.run(host=basic_info.local_ip, port=basic_info.local_port, debug=False)
