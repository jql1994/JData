# coding=utf-8
import pandas as pd
import os
import datetime
import time
import numpy as np
import math
from data_path import *
from get_actions import get_actions


# 转化年龄为level,始终用0表示未知
def convert_age(age_str):
    if age_str == u'-1':
        return -1
    elif age_str == u'15岁以下':
        return 1
    elif age_str == u'16-25岁':
        return 2
    elif age_str == u'26-35岁':
        return 3
    elif age_str == u'36-45岁':
        return 4
    elif age_str == u'46-55岁':
        return 5
    elif age_str == u'56岁以上':
        return 6
    else:
        return -1


# 把性别转化为 0-未知，1-女，2-男,始终用-1表示未知
def convert_sex(sex_col):
    if sex_col == 2:
        return -1
    else:
        sex_col


# 把注册时间转化为到2016年4月16日的注册日期
def convert_reg_days(reg_date, current_date):
    if reg_date == 0:
        return 0
    reg_date = str(reg_date)
    reg_date = time.strptime(reg_date, "%Y-%m-%d")
    reg_date = datetime.datetime(*reg_date[:3])
    current_date = time.strptime(current_date, "%Y-%m-%d")
    current_date = datetime.datetime(*current_date[:3])
    return (current_date - reg_date).days


# 注册的天数转化为等级，使用类似于对数的递减。对于注册时间在4-16之后的转为-1；0表示注册时间未知
def convert_days_to_level(days):
    reg_level = -1
    if days > 365 * 2:
        reg_level = 6
    elif days > 365:
        reg_level = 5
    elif days > 30 * 3 * 2:
        reg_level = 4
    elif days > 30 * 3 * 1:
        reg_level = 3
    elif days > 30 * 1:
        reg_level = 2
    elif days > 0:
        reg_level = 1
    elif days == 0:
        reg_level = 0
    return reg_level


# **对数衰减
def get_decay(days):
    first = [1]
    decay = range(1,len(days)+1)
    counter = 0
    for i in days:
        decay[counter] = 0.7**(2*math.log(i) + 1)
        counter +=1
    return first+decay
# -----------------------------------------------------------------


# 用户基本特征
def get_basic_user_features():
    dump_path = "../../cache/user_basic_features.csv"
    if os.path.exists(dump_path):
        return pd.read_csv(dump_path)
    else:
        user_info = pd.read_csv(user_path, encoding='gbk')
        user_info = user_info.fillna(0)
        user_info['age'] = user_info['age'].map(convert_age)
        user_info['user_reg_tm'] = user_info['user_reg_tm'].apply(convert_reg_days, current_date='2016-04-15')
        user_info['user_reg_tm'] = user_info['user_reg_tm'].apply(convert_days_to_level)
        df = pd.get_dummies(user_info['sex'], prefix='sex')
        del user_info['sex']
        user_info = pd.concat([user_info,df],axis=1)
        user_info.to_csv(dump_path, index=False)
        return user_info


# -----------------------------------------------------------------
# 用户的非衰减行为计数特征
def get_user_count_features(start_day, end_day):
    dump_path = '../../cache/uf_user_action_count_%s_%s.csv' % (start_day,end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days
        actions = get_actions(start_day,end_day)
        actions = actions[['user_id', 'type']]
        df = pd.get_dummies(actions['type'], prefix='action')
        actions = pd.concat([actions, df], axis=1)
        actions = actions.groupby(['user_id'], as_index=False).sum()
        del actions['type']
        # 有的时间段会缺少某种action，所以必须要去捕捉错误，保证所有的时间段拥有相同的行数
        try:
            actions.rename(columns=lambda x: x.replace('action_1', '%s_uf_type1_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_uf_type1_nums' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_2', '%s_uf_type2_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_uf_type2_nums' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_3', '%s_uf_type3_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_uf_type3_nums' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_4', '%s_uf_type4_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_uf_type4_nums' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_5', '%s_uf_type5_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_uf_type5_nums' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_6', '%s_uf_type6_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_uf_type6_nums' % key] = 0
            print Exception, ":", e

        actions.to_csv(dump_path, index=False)
    return actions


# 用户的衰减计数特征
def get_user_decayed_count_features(start_day, end_day):
    dump_path = '../../cache/uf_user_action_decayed_count_%s_%s.csv' % (start_day,end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        decay = get_decay(range(1,75))
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        actions = get_actions(start_day,end_day)
        counter = actions.copy()[['user_id','time','type']]
        counter['time'] = counter.loc[:, 'time'].apply(lambda x: days_in_record[x[0:10]])
        df = pd.get_dummies(counter['type'], prefix='action')
        counter = pd.concat([counter, df], axis=1)
        del counter['type']
        counter['time'] = days_in_record[end_day] - counter['time']
        counter['time'] = counter['time'].apply(lambda x:decay[x-1])
        counter['action_1'] *= counter['time']
        counter['action_2'] *= counter['time']
        counter['action_3'] *= counter['time']
        counter['action_4'] *= counter['time']
        counter['action_5'] *= counter['time']
        counter['action_6'] *= counter['time']
        del counter['time']
        counter = counter.groupby(['user_id'], as_index=False).sum()

        try:
            counter.rename(columns=lambda x: x.replace('action_1', '%s_uf_type1_dnums' % key), inplace=True)
        except Exception, e:
            counter['%s_uf_type1_dnums' % key] = 0
            print Exception, ":", e

        try:
            counter.rename(columns=lambda x: x.replace('action_2', '%s_uf_type2_dnums' % key), inplace=True)
        except Exception, e:
            counter['%s_uf_type2_dnums' % key] = 0
            print Exception, ":", e

        try:
            counter.rename(columns=lambda x: x.replace('action_3', '%s_uf_type3_dnums' % key), inplace=True)
        except Exception, e:
            counter['%s_uf_type3_dnums' % key] = 0
            print Exception, ":", e

        try:
            counter.rename(columns=lambda x: x.replace('action_4', '%s_uf_type4_dnums' % key), inplace=True)
        except Exception, e:
            counter['%s_uf_type4_dnums' % key] = 0
            print Exception, ":", e

        try:
            counter.rename(columns=lambda x: x.replace('action_5', '%s_uf_type5_dnums' % key), inplace=True)
        except Exception, e:
            counter['%s_uf_type5_dnums' % key] = 0
            print Exception, ":", e

        try:
            counter.rename(columns=lambda x: x.replace('action_6', '%s_uf_type6_dnums' % key), inplace=True)
        except Exception, e:
            counter['%s_uf_type6_dnums' % key] = 0
            print Exception, ":", e

        actions = counter
        actions.to_csv(dump_path, index=False)
    return actions

"""
# 用户非衰减，行为的type商品数特征
def get_user_count_dif_items_type(start_day,end_day):
    dump_path = '../../cache/uf_count_dif_items_type_%s_%s.csv' % (start_day,end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions = get_actions(start_day, end_day)
        counter = actions.copy()[['user_id', 'sku_id', 'type']]
        counter = counter.drop_duplicates()
        df = pd.get_dummies(counter['type'], prefix='action')
        counter = pd.concat([counter, df], axis=1)
        del counter['type']
        del counter['sku_id']
        counter = counter.groupby('user_id',as_index=False).sum()
        try:
            counter.rename(columns=lambda x: x.replace('action_1', '%s_uf_type1_nums' % key), inplace=True)
        except Exception, e:
            counter['%s_uf_type1_nums' % key] = 0
            print Exception, ":", e

        try:
            counter.rename(columns=lambda x: x.replace('action_2', '%s_uf_type2_nums' % key), inplace=True)
        except Exception, e:
            counter['%s_uf_type2_nums' % key] = 0
            print Exception, ":", e

        try:
            counter.rename(columns=lambda x: x.replace('action_3', '%s_uf_type3_nums' % key), inplace=True)
        except Exception, e:
            counter['%s_uf_type3_nums' % key] = 0
            print Exception, ":", e

        try:
            counter.rename(columns=lambda x: x.replace('action_4', '%s_uf_type4_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_uf_type4_nums' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_5', '%s_uf_type5_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_uf_type5_nums' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_6', '%s_uf_type6_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_uf_type6_nums' % key] = 0
            print Exception, ":", e
        actions.to_csv(dump_path, index=False)
    return actions
"""

# -----------------------------------------------------------------
# 用户活跃的天数
def get_user_active_days(start_day, end_day):
    dump_path = '../../cache/uf_active_days_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        actions = get_actions(start_day,end_day)
        actions['time'] = actions.loc[:, 'time'].apply(lambda x:x[0:10])
        active_days = actions.groupby(['user_id', 'time'], as_index=False).first()
        active_days = active_days.groupby(['user_id'], as_index=False).count()
        actions = active_days.loc[:, ['user_id', 'time']]
        actions.rename(columns=lambda x: x.replace('time', '%s_uf_active_days' % key), inplace=True)
        actions.to_csv(dump_path, index=False)
    return actions


# 用户交互过的商品数
def get_user_active_items(start_day, end_day):
    dump_path = '../../cache/uf_active_items_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        actions = get_actions(start_day,end_day)
        active_items = actions.groupby(['user_id', 'sku_id'], as_index=False).first()
        active_items = active_items.groupby(['user_id'], as_index=False).count()
        actions = active_items.loc[:, ['user_id', 'sku_id']]
        actions.rename(columns=lambda x: x.replace('sku_id', '%s_uf_active_items' % key), inplace=True)
        actions.to_csv(dump_path, index=False)
    return actions


# 用户交互过的品类
def get_user_active_cates(start_day, end_day):
    dump_path = '../../cache/uf_active_cates_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        actions = get_actions(start_day,end_day)
        active_cates = actions.groupby(['user_id', 'cate'], as_index=False).first()
        active_cates = active_cates.groupby(['user_id'], as_index=False).count()
        actions = active_cates.loc[:, ['user_id', 'cate']]
        actions.rename(columns=lambda x: x.replace('cate', '%s_uf_active_cates' % key), inplace=True)
        actions.to_csv(dump_path, index=False)
    return actions


# 用户交互过的品牌
def get_user_active_brands(start_day, end_day):
    dump_path = '../../cache/uf_active_brands_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        actions = get_actions(start_day,end_day)
        active_brands = actions.groupby(['user_id', 'brand'], as_index=False).first()
        active_brands = active_brands.groupby(['user_id'], as_index=False).count()
        actions = active_brands.loc[:, ['user_id', 'brand']]
        actions.rename(columns=lambda x: x.replace('brand', '%s_uf_active_brands' % key), inplace=True)
        actions.to_csv(dump_path, index=False)
    return actions


# -----------------------------------------------------------------
# 用户购买前的交互时间特征
def get_user_time_features(start_day, end_day):
    dump_path = '../../cache/uf_action_time_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions = get_actions(start_day, end_day)
        actions['time'] = actions.loc[:, 'time'].apply(lambda x: days_in_record[x[0:10]])
        actions_time = actions[['user_id', 'sku_id', 'time', 'type', 'cate']]
        # 一种很智障的去重方法，找到用户当天进行的操作记录
        actions_time = actions_time.groupby(['user_id', 'sku_id', 'time', 'type'], as_index=False).count()
        del actions_time['cate']
        # 找到用户购买商品的时间，只要最早的一次
        end_time = actions_time[actions_time.type==4].groupby(['user_id', 'sku_id'], as_index=False).apply(lambda x: x[x['time']==x['time'].min()])
        # 这个表里有购买时间的信息
        end_time = end_time[['user_id','sku_id','time']].reset_index(drop=True)
        end_time.rename(columns=lambda x: x.replace('time', 'buy_time'), inplace=True)
        # 把购买的信息引入进来，没买的直接就过滤掉了
        actions_time = pd.merge(actions_time,end_time,how='left',on=['user_id', 'sku_id'])
        del actions_time['type']
        actions_time = actions_time.dropna()
        # 因为用户可能在当天有多个操作，所以去除重复
        actions_time = actions_time.drop_duplicates()
        # 找到现在这个表里最早的时间，也就是最早操作的时间
        start_time = actions_time.groupby(['user_id', 'sku_id'],as_index =False).apply(lambda x: x[x['time']==x['time'].min()])
        start_time = start_time.reset_index(drop=True)
        # 这个表里信息基本就全了
        actions = start_time[['user_id','time','buy_time']]
        actions['uf_start_days'] = actions['buy_time'] - actions['time']
        actions = actions[['user_id','uf_start_days']]
        actions = actions.groupby(['user_id'], as_index=False).mean()
        actions.to_csv(dump_path, index=False)
    return actions


# -----------------------------------------------------------------
# User的隔天转化率特征
def get_user_trans_features_next(start_day, end_day):
    dump_path = '../../cache/uf_action_trans_next_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions = get_actions(start_day, end_day)
        actions['time'] = actions.loc[:, 'time'].apply(lambda x: days_in_record[x[0:10]])
        actions_ratio = actions[['user_id', 'sku_id', 'time', 'type']]
        actions_ratio = actions_ratio.drop_duplicates()
        actions_ratio_type2 = actions_ratio[actions_ratio['type'] == 2]
        actions_ratio_type5 = actions_ratio[actions_ratio['type'] == 5]
        actions_ratio_type4 = actions_ratio[actions_ratio['type'] == 4]
        actions_ratio_type4.rename(columns=lambda x: x.replace('type', 'flag'), inplace=True)
        actions_ratio_type4_forward = actions_ratio_type4.copy()
        actions_ratio_type4_forward['flag'] = 1
        actions_ratio_type4_forward['time'] -= 1
        actions_ratio_type2_next = actions_ratio_type2.copy()
        # 找到type2 和 type2 之后的操作
        actions_ratio_type2_next = pd.merge(actions_ratio_type2_next, actions_ratio_type4_forward, how='left',
                                            on=['user_id', 'sku_id', 'time'])
        # 如果flag是1说明type2之后，第二天买了，否则是0
        actions_ratio_type2_next = actions_ratio_type2_next[['user_id', 'flag']]
        # 补0
        actions_ratio_type2_next = actions_ratio_type2_next.fillna(0)
        # 对于用户所有type2的商品，看看flag的平均值是多少
        actions_type2_ratio = actions_ratio_type2_next.groupby('user_id', as_index=False).mean()
        # 结果保存在type2——ratio中
        actions_type2_ratio.rename(columns=lambda x: x.replace('flag', 'uf_type2_trans_next'), inplace=True)

        actions_ratio_type5_next = actions_ratio_type5.copy()
        actions_ratio_type5_next = pd.merge(actions_ratio_type5_next, actions_ratio_type4_forward, how='left',
                                            on=['user_id', 'sku_id', 'time'])
        actions_ratio_type5_next = actions_ratio_type5_next[['user_id', 'flag']]
        actions_ratio_type5_next = actions_ratio_type5_next.fillna(0)
        # 这个特征比较稀疏
        actions_type5_ratio = actions_ratio_type5_next.groupby('user_id', as_index=False).mean()
        actions_type5_ratio.rename(columns=lambda x: x.replace('flag', 'uf_type5_trans_next'), inplace=True)
        actions_ratio = actions[['user_id']].drop_duplicates()
        actions_ratio = pd.merge(actions_ratio, actions_type2_ratio, how='left', on=['user_id'])
        actions_ratio = pd.merge(actions_ratio, actions_type5_ratio, how='left', on=['user_id'])
        actions = actions_ratio
        actions.to_csv(dump_path, index=False)
    return actions


# User的转化率特征
def get_user_trans_features(start_day, end_day):
    # features = ['user_id', 'uf_type1_trans', 'uf_type2_trans', 'uf_type3_trans', 'uf_type5_trans',
    #             'uf_type6_trans']
    dump_path = '../../cache/uf_action_trans_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions = get_actions(start_day, end_day)
        actions_trans = actions[['user_id', 'sku_id', 'type']]
        # 去掉重复type，我只需要知道用户对商品进行过哪些操作
        actions_trans = actions_trans.drop_duplicates()
        df = pd.get_dummies(actions_trans['type'], prefix='action')
        actions_trans = pd.concat([actions_trans, df], axis=1)
        del actions_trans['type']
        # 统计一个表，每一行就可以知道这件商品进行过哪些操作
        actions_trans = actions_trans.groupby(['user_id', 'sku_id'],as_index=False).sum()
        # 找到操作不为0的子表去找他们对于购买的转化率
        actions_trans_type1 = actions_trans[actions_trans['action_1'] > 0]
        actions_trans_type2 = actions_trans[actions_trans['action_2'] > 0]
        actions_trans_type3 = actions_trans[actions_trans['action_3'] > 0]
        actions_trans_type5 = actions_trans[actions_trans['action_5'] > 0]
        actions_trans_type6 = actions_trans[actions_trans['action_6'] > 0]
        # 计算用户所有操作过的商品的转化率特征
        actions_trans_type1 = actions_trans_type1.groupby('user_id',as_index=False).sum()
        actions_trans_type2 = actions_trans_type2.groupby('user_id',as_index=False).sum()
        actions_trans_type3 = actions_trans_type3.groupby('user_id',as_index=False).sum()
        actions_trans_type5 = actions_trans_type5.groupby('user_id',as_index=False).sum()
        actions_trans_type6 = actions_trans_type6.groupby('user_id',as_index=False).sum()
        actions_trans_type1['uf_type1_trans'] = actions_trans_type1['action_4']/actions_trans_type1['action_1']
        actions_trans_type2['uf_type2_trans'] = actions_trans_type2['action_4']/actions_trans_type2['action_2']
        actions_trans_type3['uf_type3_trans'] = actions_trans_type3['action_4']/actions_trans_type3['action_3']
        actions_trans_type5['uf_type5_trans'] = actions_trans_type5['action_4']/actions_trans_type5['action_5']
        actions_trans_type6['uf_type6_trans'] = actions_trans_type6['action_4']/actions_trans_type6['action_6']
        actions_trans_type1 = actions_trans_type1[['user_id','uf_type1_trans']]
        actions_trans_type2 = actions_trans_type2[['user_id','uf_type2_trans']]
        actions_trans_type3 = actions_trans_type3[['user_id','uf_type3_trans']]
        actions_trans_type5 = actions_trans_type5[['user_id','uf_type5_trans']]
        actions_trans_type6 = actions_trans_type6[['user_id','uf_type6_trans']]
        actions_trans = actions_trans[['user_id']]
        actions_trans = actions_trans.drop_duplicates()
        actions_trans = pd.merge(actions_trans, actions_trans_type1, how='left', on=['user_id'])
        actions_trans = pd.merge(actions_trans, actions_trans_type2, how='left', on=['user_id'])
        actions_trans = pd.merge(actions_trans, actions_trans_type3, how='left', on=['user_id'])
        actions_trans = pd.merge(actions_trans, actions_trans_type5, how='left', on=['user_id'])
        actions_trans = pd.merge(actions_trans, actions_trans_type6, how='left', on=['user_id'])
        actions = actions_trans
        actions.to_csv(dump_path, index=False)
    return actions


# user比例特征
def get_user_ratio_features(start_day, end_day):
    features = ['user_id', 'uf_type1_ratio', 'uf_type2_ratio', 'uf_type3_ratio', 'uf_type5_ratio',
                'uf_type6_ratio']
    dump_path = '../../cache/uf_type_nums_ratio_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        actions = get_user_count_features(start_day, end_day)
        actions['uf_type1_ratio'] = actions['%s_uf_type4_nums' % key] / actions['%s_uf_type1_nums' % key]
        actions['uf_type2_ratio'] = actions['%s_uf_type4_nums' % key] / actions['%s_uf_type2_nums' % key]
        actions['uf_type3_ratio'] = actions['%s_uf_type4_nums' % key] / actions['%s_uf_type3_nums' % key]
        actions['uf_type5_ratio'] = actions['%s_uf_type4_nums' % key] / actions['%s_uf_type5_nums' % key]
        actions['uf_type6_ratio'] = actions['%s_uf_type4_nums' % key] / actions['%s_uf_type6_nums' % key]
        actions = actions[features]
        actions.to_csv(dump_path, index=False)
    return actions


# -------------------------------------------------------------
def get_user_features(start_day, end_day):
    if days_in_record[end_day]-days_in_record[start_day] < 29:
        print "时间太短"
        exit()
    dump_path = '../../cache/user_all_features_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        # 生成时间间隔 15,7，3
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = dt_end_day - datetime.timedelta(days=15)
        start_15day = dt_start_day.strftime('%Y-%m-%d')
        dt_start_day = dt_end_day - datetime.timedelta(days=7)
        start_7day = dt_start_day.strftime('%Y-%m-%d')
        dt_start_day = dt_end_day - datetime.timedelta(days=3)
        start_3day = dt_start_day.strftime('%Y-%m-%d')
        # 计数特征
        print "开始计算用户特征"
        u_features = get_basic_user_features()
        print "计数特征计算中"
        print "计算 %s_%s 计数" % (start_day, end_day)
        uf_counter = get_user_count_features(start_day,end_day)
        u_features = pd.merge(u_features,uf_counter,how='left',on='user_id')
        uf_decayed_counter = get_user_decayed_count_features(start_day,end_day)
        u_features = pd.merge(u_features,uf_decayed_counter,how='left',on='user_id')

        print "3,7,15"
        # 计算的是额外特征，可以删掉
        uf_counter = get_user_count_features(start_15day,end_day)
        u_features = pd.merge(u_features,uf_counter,how='left',on='user_id')
        uf_counter = get_user_count_features(start_7day,end_day)
        u_features = pd.merge(u_features,uf_counter,how='left',on='user_id')
        uf_counter = get_user_count_features(start_3day,end_day)
        u_features = pd.merge(u_features,uf_counter,how='left',on='user_id')

        # 活跃度特征
        print "活跃度特征计算中"
        uf_active_days = get_user_active_days(start_day,end_day)
        uf_active_items = get_user_active_items(start_day,end_day)
        uf_active_brands = get_user_active_brands(start_day,end_day)
        uf_active_cates = get_user_active_cates(start_day,end_day)
        u_features = pd.merge(u_features,uf_active_days,how='left',on='user_id')
        u_features = pd.merge(u_features,uf_active_brands,how='left',on='user_id')
        u_features = pd.merge(u_features,uf_active_cates,how='left',on='user_id')
        u_features = pd.merge(u_features,uf_active_items,how='left',on='user_id')

        print "3,7"
        uf_active_days = get_user_active_days(start_3day,end_day)
        uf_active_items = get_user_active_items(start_3day,end_day)
        uf_active_brands = get_user_active_brands(start_3day,end_day)
        uf_active_cates = get_user_active_cates(start_3day,end_day)
        u_features = pd.merge(u_features,uf_active_days,how='left',on='user_id')
        u_features = pd.merge(u_features,uf_active_brands,how='left',on='user_id')
        u_features = pd.merge(u_features,uf_active_cates,how='left',on='user_id')
        u_features = pd.merge(u_features,uf_active_items,how='left',on='user_id')

        uf_active_days = get_user_active_days(start_7day,end_day)
        uf_active_items = get_user_active_items(start_7day,end_day)
        uf_active_brands = get_user_active_brands(start_7day,end_day)
        uf_active_cates = get_user_active_cates(start_7day,end_day)
        u_features = pd.merge(u_features,uf_active_days,how='left',on='user_id')
        u_features = pd.merge(u_features,uf_active_brands,how='left',on='user_id')
        u_features = pd.merge(u_features,uf_active_cates,how='left',on='user_id')
        u_features = pd.merge(u_features,uf_active_items,how='left',on='user_id')

        u_features = u_features.fillna(0)

        print "平均购买间隔"
        uf_time = get_user_time_features(start_day,end_day)
        print "计算转化率及比值"
        uf_trans_next = get_user_trans_features_next(start_day,end_day)
        uf_trans = get_user_trans_features(start_day,end_day)
        uf_ratio = get_user_ratio_features(start_day,end_day)
        u_features = pd.merge(u_features,uf_time,how='left',on='user_id')
        u_features = pd.merge(u_features,uf_trans,how='left',on='user_id')
        u_features = pd.merge(u_features,uf_ratio,how='left',on='user_id')
        u_features = pd.merge(u_features,uf_trans_next,how='left',on='user_id')

        u_features = u_features.fillna(-1)

        actions = u_features
        actions.to_csv(dump_path, index=False)
    return actions

# -------------------------------------------------------------
# if __name__ == '__main__':
#     # start_date = '2016-02-19'
#     # end_date = '2016-03-08'
#     # get_user_count_features(start_date,end_date)
#     # this = get_user_decayed_count_features(start_date,end_date)
#     # print this.columns
#     # this = get_user_active_days(start_date,end_date)
#     # print this.columns
#     # this = get_user_active_brands(start_date,end_date)
#     # print this.columns
#     # this = get_user_active_cates(start_date,end_date)
#     # print this.columns
#     # this = get_user_active_items(start_date,end_date)
#     # print this.columns
#     # this = get_user_time_features(start_date,end_date)
#     # print this.columns
#     # this = get_user_trans_features(start_date,end_date)
#     # print this.columns
#     # this = get_user_trans_features_next(start_date,end_date)
#     # print this.columns
#     # this = get_user_ratio_features(start_date,end_date)
#     # print this.columns
#     get_user_features(start_date,end_date)