#!usr/bin/env python
# -*- coding:utf-8 _*-
"""
@author:jiaruya
@file: interface.py
@time: 2019/01/22
@desc:
"""
from flask import request
from flask_restplus import Resource, reqparse

CODE_SUCCESS = 1000
CODE_PARAM_ERR = 1001
CODE_CHECK_ERR = 1002
CODE_OP_FAIL = 1003

REST_VERSION = '/version'                               # 服务版本
REST_CONFIG = '/config'                                 # 服务配置
REST_READ_TOPIC = '/data/topic'                         # 按照主题读取数据
REST_READ_TAG = '/data/tag'                             # 按照测点读取数据
REST_WRITE_TAG = '/data/tag'                            # 按照测点写入数据
REST_FLUSH_DATA = '/data/flush'                         # 清空数据
REST_STATE = '/state'                                   # 获取状态


def make_http_ret(ret, msg):
    if ret:
        return {'data': msg, 'code': CODE_SUCCESS, 'msg': '操作成功'}, 200
    else:
        return {'data': msg, 'code': CODE_OP_FAIL, 'msg': '操作失败'}, 400


class IVersion(Resource):
    def get(self):
        ret, msg = self.get_version()
        return make_http_ret(ret, msg)

    def get_version(self):
        pass


class IConfig(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('cfg_name', type=str, required=True, location='args')
        args = parser.parse_args()
        ret, msg = self.get_config(args['cfg_name'])
        return make_http_ret(ret, msg)

    def post(self):
        req = request.json
        if req is None or 'cfg_name' not in req or 'cfg_msg' not in req:
            return {'data': '', 'code': CODE_PARAM_ERR, 'msg': '入参错误'}, REST_FAIL
        cfg_name = req['cfg_name']
        cfg_msg = req['cfg_msg']
        ret, msg = self.set_config(cfg_name, cfg_msg)
        return make_http_ret(ret, msg)

    def get_config(self, cfg_name):
        pass

    def set_config(self, cfg_name, cfg_msg):
        pass


class IReadTopic(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('topic', type=str, required=True, location='args')
        args = parser.parse_args()
        topic = args['topic']
        ret, msg = self.read_topic(topic)
        return make_http_ret(ret, msg)

    def read_topic(self, topic):
        pass


class IReadTag(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('tags', type=str, required=True, location='args')
        args = parser.parse_args()
        tags = args['tags'].split(',')
        ret, msg = self.read_tags(tags)
        return make_http_ret(ret, msg)

    def read_tags(self, tags):
        pass


class IWriteTag(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('tag', type=str, required=True, location='json')
        parser.add_argument('value_double', type=float, required=True, location='json')
        parser.add_argument('value_int64', type=int, required=True, location='json')
        parser.add_argument('value_string', type=str, required=True, location='json')
        parser.add_argument('value_bytes', type=str, required=True, location='json')
        parser.add_argument('data_type', type=int, required=True, location='json')
        parser.add_argument('quality', type=int, required=True, location='json')
        parser.add_argument('timestamp', type=int, required=True, location='json')
        args = parser.parse_args()
        ret, msg = self.write_data(args['tag'], args['value_double'], args['value_int64'], args['value_string'],
                                   args['value_bytes'], args['data_type'], args['quality'], args['timestamp'])
        return make_http_ret(ret, msg)

    def write_data(self, tag, value_double, value_int64, value_string, value_bytes, data_type, quality, timestamp):
        pass


class IFlushData(Resource):
    def get(self):
        ret, msg = self.flush_data()
        return make_http_ret(ret, msg)

    def flush_data(self):
        pass


class IState(Resource):
    def get(self):
        ret, msg = self.get_state()
        return make_http_ret(ret, msg)

    def get_state(self):
        pass