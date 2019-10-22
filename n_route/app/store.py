#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: jiaruya
@file: store.py
@time: 2019/08/28
@desc:数据缓存区
"""
import queue


# 连续数据
class Sequence:
    def __init__(self, max_length):
        # 连续数据：数组
        self.__series_list = []
        # 数据索引字典
        self.__tag_index_dict = {}
        # 数据长度
        self.__data_count = -1
        # 最大数组个数
        self.__max_length = max_length

    # 写入单个数据
    def put_data(self, key, data):
        try:
            index = self.__tag_index_dict[key]
            self.__series_list[index] = data
        except:
            index = len(self.__series_list)
            # 当数据量大于设置的最大数据量时，写入失败
            if index > self.__max_length:
                return -1
            self.__series_list.append(data)
            self.__tag_index_dict[key] = index
            self.__data_count = index + 1
        return index

    # 根据标签点名称集合获取索引位置集合
    def get_index(self, key):
        if key in self.__tag_index_dict:
            return self.__tag_index_dict[key]
        return -1

    # 读取单个数据（按照索引）
    def get_data(self, index):
        if index < 0 or index >= self.__data_count:
            return None
        return self.__series_list[index]

    # 读取全部数据
    def get_data_all(self):
        return self.__series_list

    # 获取数据长度
    def get_data_len(self):
        return self.__data_count

    # 清空数据
    def clear(self):
        self.__series_list.clear()
        self.__data_count = 0


# 离散数据
class Discrete:
    def __init__(self, max_length):
        # 最大数据个数
        self.__max_length = max_length
        # 离散数据队列
        self.__discrete_queue = queue.Queue(self.__max_length)

    # 写入单个数据
    def put_data(self, data):
        self.__discrete_queue.put(data, block=True)

    # 读取单个数据
    def get_data(self):
        try:
            return self.__discrete_queue.get(block=True, timeout=5)
        except Exception:
            return None

    # 读取全部数据
    def get_data_all(self):
        data_list = []
        while not self.__discrete_queue.empty():
            data_list.append(self.__discrete_queue.get())
        return data_list

    # 获取数据长度
    def get_data_len(self):
        return self.__discrete_queue.qsize()

    # 清空数据
    def clear(self):
        while not self.__discrete_queue.empty():
            self.__discrete_queue.get()

