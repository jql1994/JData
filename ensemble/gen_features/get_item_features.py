# coding=utf-8
import pandas as pd
import os
import datetime
import time
import math
import numpy as np
from data_path import *
from get_actions import get_actions


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
# 获取基础item特征
def get_basic_item_features():
    dump_path = '../../cache/product_basic_features.csv'
    if os.path.exists(dump_path):
        product = pd.read_csv(dump_path)
    else:
        product = pd.read_csv(product_path)
        attr1_df = pd.get_dummies(product["a1"], prefix="a1")
        attr2_df = pd.get_dummies(product["a2"], prefix="a2")
        attr3_df = pd.get_dummies(product["a3"], prefix="a3")
        product = pd.concat([product[['sku_id', 'cate', 'brand']], attr1_df, attr2_df, attr3_df], axis=1)
        product.to_csv(dump_path, index=False)
    return product


# -----------------------------------------------------------------
# Item的非衰减action计数特征
def get_item_count_features(start_day, end_day):
    dump_path = '../../cache/if_action_count_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        actions = get_actions(start_day, end_day)
        actions = actions[['sku_id', 'type']]
        df = pd.get_dummies(actions['type'], prefix='action')
        actions = pd.concat([actions, df], axis=1)
        actions = actions.groupby(['sku_id'], as_index=False).sum()
        del actions['type']
        try:
            actions.rename(columns=lambda x: x.replace('action_1', '%s_if_type1_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_if_type1_nums' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_2', '%s_if_type2_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_if_type2_nums' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_3', '%s_if_type3_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_if_type3_nums' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_4', '%s_if_type4_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_if_type4_nums' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_5', '%s_if_type5_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_if_type5_nums' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_6', '%s_if_type6_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_if_type6_nums' % key] = 0
            print Exception, ":", e

        actions.to_csv(dump_path, index=False)
    return actions


# Item的user计数特征
def get_item_count_user_features(start_day, end_day):
    dump_path = '../../cache/if_user_count_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        actions = get_actions(start_day,end_day)
        actions = actions[['sku_id', 'user_id', 'type']]
        actions = actions.drop_duplicates()
        df = pd.get_dummies(actions['type'], prefix='action')
        actions = pd.concat([actions, df], axis=1)
        del actions['user_id']
        del actions['type']
        actions = actions.groupby(['sku_id'], as_index=False).sum()
        try:
            actions.rename(columns=lambda x: x.replace('action_1', '%s_if_type1_user' % key), inplace=True)
        except Exception, e:
            actions['%s_if_type1_user' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_2', '%s_if_type2_user' % key), inplace=True)
        except Exception, e:
            actions['%s_if_type2_user' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_3', '%s_if_type3_user' % key), inplace=True)
        except Exception, e:
            actions['%s_if_type3_user' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_4', '%s_if_type4_user' % key), inplace=True)
        except Exception, e:
            actions['%s_if_type4_user' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_5', '%s_if_type5_user' % key), inplace=True)
        except Exception, e:
            actions['%s_if_type5_user' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_6', '%s_if_type6_user' % key), inplace=True)
        except Exception, e:
            actions['%s_if_type6_user' % key] = 0
            print Exception, ":", e

        actions.to_csv(dump_path, index=False)
    return actions


# Item的衰减action计数特征
def get_item_decayed_count_features(start_day, end_day):
    dump_path = '../../cache/if_action_decayed_count_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        decay = get_decay(range(1,75))
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        actions = get_actions(start_day,end_day)
        counter = actions.copy()[['sku_id','time','type']]
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
        counter = counter.groupby(['sku_id'], as_index=False).sum()

        try:
            counter.rename(columns=lambda x: x.replace('action_1', '%s_if_type1_dnums' % key), inplace=True)
        except Exception, e:
            counter['%s_if_type1_dnums' % key] = 0
            print Exception, ":", e

        try:
            counter.rename(columns=lambda x: x.replace('action_2', '%s_if_type2_dnums' % key), inplace=True)
        except Exception, e:
            counter['%s_if_type2_dnums' % key] = 0
            print Exception, ":", e

        try:
            counter.rename(columns=lambda x: x.replace('action_3', '%s_if_type3_dnums' % key), inplace=True)
        except Exception, e:
            counter['%s_if_type3_dnums' % key] = 0
            print Exception, ":", e

        try:
            counter.rename(columns=lambda x: x.replace('action_4', '%s_if_type4_dnums' % key), inplace=True)
        except Exception, e:
            counter['%s_if_type4_dnums' % key] = 0
            print Exception, ":", e

        try:
            counter.rename(columns=lambda x: x.replace('action_5', '%s_if_type5_dnums' % key), inplace=True)
        except Exception, e:
            counter['%s_if_type5_dnums' % key] = 0
            print Exception, ":", e

        try:
            counter.rename(columns=lambda x: x.replace('action_6', '%s_if_type6_dnums' % key), inplace=True)
        except Exception, e:
            counter['%s_if_type6_dnums' % key] = 0
            print Exception, ":", e

        actions = counter
        actions.to_csv(dump_path, index=False)
    return actions


# -----------------------------------------------------------------
# 商品活跃的天数
def get_item_active_days(start_day, end_day):
    dump_path = '../../cache/if_active_days_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        actions = get_actions(start_day,end_day)
        actions['time'] = actions.loc[:, 'time'].apply(lambda x:x[0:10])
        active_days = actions.groupby(['sku_id', 'time'], as_index=False).first()
        active_days = active_days.groupby(['sku_id'], as_index=False).count()
        actions = active_days.loc[:, ['sku_id', 'time']]
        actions.rename(columns=lambda x: x.replace('time', '%s_if_active_days' % key), inplace=True)
        actions.to_csv(dump_path, index=False)
    return actions


# 商品交互过的用户数
def get_item_active_users(start_day, end_day):
    dump_path = '../../cache/if_active_users_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        actions = get_actions(start_day,end_day)
        active_items = actions.copy()[['user_id','sku_id']]
        active_items = active_items.drop_duplicates()
        active_items = active_items.groupby(['sku_id'], as_index=False).count()
        actions = active_items.loc[:, ['user_id', 'sku_id']]
        actions.rename(columns=lambda x: x.replace('user_id', '%s_if_active_users' % key), inplace=True)
        actions.to_csv(dump_path, index=False)
    return actions


# 计算重复购买率 检查！！！！
def get_item_repeat_buy(start_day, end_day):
    dump_path = '../../cache/if_repeat_buy_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions = get_actions(start_day, end_day)
        actions_repeat = actions[['user_id', 'type', 'sku_id']]
        # 找出所有的购买
        actions_repeat = actions_repeat[actions_repeat['type'] == 4]
        actions_repeat_count = actions_repeat.groupby(['sku_id', 'user_id'],as_index=False).count()
        del actions_repeat_count['type']
        # 得到每个商品的用户数
        actions_repeat_count = actions_repeat_count.groupby(['sku_id'], as_index=False).count()
        # 计算所有的用户在item上购买的数量，得到的是每个商品的销量
        actions_repeat = actions_repeat.groupby(['sku_id'], as_index=False).count()
        actions_repeat['if_repeat_buys'] = (actions_repeat['type'] - actions_repeat_count['user_id']) / actions_repeat['type']
        actions = actions_repeat[['sku_id', 'if_repeat_buys']]
        actions.to_csv(dump_path, index=False)
    return actions


# -----------------------------------------------------------------
# 计算Item的转化率特征
def get_item_trans_features(start_day, end_day):
    dump_path = '../../cache/if_action_trans_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions = get_actions(start_day,end_day)
        actions_trans = actions[['user_id', 'sku_id', 'type']]
        # 去掉重复type，只需要商品被哪些用户进行了哪些操作
        actions_trans = actions_trans.drop_duplicates()
        df = pd.get_dummies(actions_trans['type'], prefix='action')
        actions_trans = pd.concat([actions_trans, df], axis=1)
        del actions_trans['type']

        # 统计一个表，每一行就可以知道这件商品进行过哪些操作
        actions_trans = actions_trans.groupby(['sku_id', 'user_id'], as_index=False).sum()
        # 找到操作不为0的子表去找他们对于购买的转化率
        actions_trans_type1 = actions_trans[actions_trans['action_1'] > 0]
        actions_trans_type2 = actions_trans[actions_trans['action_2'] > 0]
        actions_trans_type3 = actions_trans[actions_trans['action_3'] > 0]
        actions_trans_type5 = actions_trans[actions_trans['action_5'] > 0]
        actions_trans_type6 = actions_trans[actions_trans['action_6'] > 0]
        # 计算cate所有操作过的商品的转化率特征
        actions_trans_type1 = actions_trans_type1.groupby('sku_id', as_index=False).sum()
        actions_trans_type2 = actions_trans_type2.groupby('sku_id', as_index=False).sum()
        actions_trans_type3 = actions_trans_type3.groupby('sku_id', as_index=False).sum()
        actions_trans_type5 = actions_trans_type5.groupby('sku_id', as_index=False).sum()
        actions_trans_type6 = actions_trans_type6.groupby('sku_id', as_index=False).sum()

        actions_trans_type1['if_type1_trans'] = actions_trans_type1['action_4'] / actions_trans_type1['action_1']
        actions_trans_type2['if_type2_trans'] = actions_trans_type2['action_4'] / actions_trans_type2['action_2']
        actions_trans_type3['if_type3_trans'] = actions_trans_type3['action_4'] / actions_trans_type3['action_3']
        actions_trans_type5['if_type5_trans'] = actions_trans_type5['action_4'] / actions_trans_type5['action_5']
        actions_trans_type6['if_type6_trans'] = actions_trans_type6['action_4'] / actions_trans_type6['action_6']
        actions_trans_type1 = actions_trans_type1[['sku_id', 'if_type1_trans']]
        actions_trans_type2 = actions_trans_type2[['sku_id', 'if_type2_trans']]
        actions_trans_type3 = actions_trans_type3[['sku_id', 'if_type3_trans']]
        actions_trans_type5 = actions_trans_type5[['sku_id', 'if_type5_trans']]
        actions_trans_type6 = actions_trans_type6[['sku_id', 'if_type6_trans']]
        actions_trans = actions_trans[['sku_id']]
        actions_trans = actions_trans.drop_duplicates()
        actions_trans = pd.merge(actions_trans, actions_trans_type1, how='left', on=['sku_id'])
        actions_trans = pd.merge(actions_trans, actions_trans_type2, how='left', on=['sku_id'])
        actions_trans = pd.merge(actions_trans, actions_trans_type3, how='left', on=['sku_id'])
        actions_trans = pd.merge(actions_trans, actions_trans_type5, how='left', on=['sku_id'])
        actions_trans = pd.merge(actions_trans, actions_trans_type6, how='left', on=['sku_id'])
        actions = actions_trans
        actions.to_csv(dump_path, index=False)
    return actions


# Item的隔天转化率特征
def get_item_trans_features_next(start_day, end_day):
    dump_path = '../../cache/if_action_trans_next_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions = get_actions(start_day,end_day)
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
        actions_ratio_type2_next = actions_ratio_type2_next[['sku_id', 'flag']]
        # 补0
        actions_ratio_type2_next = actions_ratio_type2_next.fillna(0)
        # 对于用户所有type2的商品，看看flag的平均值是多少
        actions_type2_ratio = actions_ratio_type2_next.groupby('sku_id', as_index=False).mean()
        # 结果保存在type2——ratio中
        actions_type2_ratio.rename(columns=lambda x: x.replace('flag', 'if_type2_trans_next'), inplace=True)

        actions_ratio_type5_next = actions_ratio_type5.copy()
        actions_ratio_type5_next = pd.merge(actions_ratio_type5_next, actions_ratio_type4_forward, how='left',
                                            on=['user_id', 'sku_id', 'time'])
        actions_ratio_type5_next = actions_ratio_type5_next[['sku_id', 'flag']]
        actions_ratio_type5_next = actions_ratio_type5_next.fillna(0)
        # 这个特征比较稀疏
        actions_type5_ratio = actions_ratio_type5_next.groupby('sku_id', as_index=False).mean()
        actions_type5_ratio.rename(columns=lambda x: x.replace('flag', 'if_type5_trans_next'), inplace=True)
        actions_ratio = actions[['sku_id']].drop_duplicates()
        actions_ratio = pd.merge(actions_ratio, actions_type2_ratio, how='left', on=['sku_id'])
        actions_ratio = pd.merge(actions_ratio, actions_type5_ratio, how='left', on=['sku_id'])
        actions = actions_ratio
        actions.to_csv(dump_path, index=False)
    return actions


# 计算Item的比值特征
def get_item_ratio_features(start_day, end_day):
    features = ['sku_id', 'if_type1_ratio', 'if_type2_ratio', 'if_type3_ratio','if_type5_ratio','if_type6_ratio'
                , 'if_type1_user_ratio', 'if_type2_user_ratio', 'if_type3_user_ratio','if_type5_user_ratio','if_type6_user_ratio']
    dump_path = '../../cache/if_action_ratio_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        # 计算用户的转化率
        actions = get_item_count_user_features(start_day,end_day)
        actions['if_type1_user_ratio'] = actions['%s_if_type4_user' % key] / actions['%s_if_type1_user' % key]
        actions['if_type2_user_ratio'] = actions['%s_if_type4_user' % key] / actions['%s_if_type2_user' % key]
        actions['if_type3_user_ratio'] = actions['%s_if_type4_user' % key] / actions['%s_if_type3_user' % key]
        actions['if_type5_user_ratio'] = actions['%s_if_type4_user' % key] / actions['%s_if_type5_user' % key]
        actions['if_type6_user_ratio'] = actions['%s_if_type4_user' % key] / actions['%s_if_type6_user' % key]
        # 计算操作的转化率
        actions_num = get_item_count_features(start_day,end_day)
        actions_num['if_type1_ratio'] = actions_num['%s_if_type4_nums' % key] / actions_num['%s_if_type4_nums' % key]
        actions_num['if_type2_ratio'] = actions_num['%s_if_type4_nums' % key] / actions_num['%s_if_type4_nums' % key]
        actions_num['if_type3_ratio'] = actions_num['%s_if_type4_nums' % key] / actions_num['%s_if_type4_nums' % key]
        actions_num['if_type5_ratio'] = actions_num['%s_if_type4_nums' % key] / actions_num['%s_if_type4_nums' % key]
        actions_num['if_type6_ratio'] = actions_num['%s_if_type4_nums' % key] / actions_num['%s_if_type4_nums' % key]

        actions = pd.merge(actions, actions_num, how='left', on=['sku_id'])
        actions = actions[features]
        actions.to_csv(dump_path, index=False)
    return actions


# -----------------------------------------------------------------
# 计算Item的无衰减排名特征
def get_item_action_rank_features(start_day, end_day):
    dump_path = '../../cache/if_action_rank_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        features = ['sku_id', '%s_if_action_rank' % key, '%s_if_purchase_rank' % key]

        action_rank = get_item_count_features(start_day,end_day)
        action_rank['actions'] = 0
        action_rank['actions'] += action_rank['%s_if_type1_nums' % key]
        action_rank['actions'] += action_rank['%s_if_type2_nums' % key]
        action_rank['actions'] += action_rank['%s_if_type3_nums' % key]
        action_rank['actions'] += action_rank['%s_if_type4_nums' % key]
        action_rank['actions'] += action_rank['%s_if_type5_nums' % key]
        action_rank['actions'] += action_rank['%s_if_type6_nums' % key]

        action_rank['%s_if_action_rank' % key] = action_rank.rank(ascending=False,method='min')['actions']
        action_rank['%s_if_purchase_rank' % key] = action_rank.rank(ascending=False,method='min')['%s_if_type2_nums' % key]

        actions = action_rank[features]
        actions.to_csv(dump_path, index=False)
    return actions


# 计算Item的衰减排名特征
def get_item_decayed_action_rank_features(start_day, end_day):
    dump_path = '../../cache/if_decayed_action_rank_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        features = ['sku_id', '%s_if_decayed_action_rank' % key, '%s_if_decayed_purchase_rank' % key]

        action_rank = get_item_decayed_count_features(start_day,end_day)
        action_rank['actions'] = 0
        action_rank['actions'] += action_rank['%s_if_type1_dnums' % key]
        action_rank['actions'] += action_rank['%s_if_type2_dnums' % key]
        action_rank['actions'] += action_rank['%s_if_type3_dnums' % key]
        action_rank['actions'] += action_rank['%s_if_type4_dnums' % key]
        action_rank['actions'] += action_rank['%s_if_type5_dnums' % key]
        action_rank['actions'] += action_rank['%s_if_type6_dnums' % key]

        action_rank['%s_if_decayed_action_rank' % key] = action_rank.rank(ascending=False,method='min')['actions']
        action_rank['%s_if_decayed_purchase_rank' % key] = action_rank.rank(ascending=False,method='min')['%s_if_type2_dnums' % key]

        actions = action_rank[features]
        actions.to_csv(dump_path, index=False)
    return actions


# 计算Item的用户热度排名
def get_item_user_rank_features(start_day, end_day):
    dump_path = '../../cache/if_user_rank_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        features = ['sku_id', '%s_if_user_rank' % key, '%s_if_purchase_user_rank' % key]

        action_rank = get_item_count_user_features(start_day,end_day)
        action_rank['actions'] = 0
        action_rank['actions'] += action_rank['%s_if_type1_user' % key]
        action_rank['actions'] += action_rank['%s_if_type2_user' % key]
        action_rank['actions'] += action_rank['%s_if_type3_user' % key]
        action_rank['actions'] += action_rank['%s_if_type4_user' % key]
        action_rank['actions'] += action_rank['%s_if_type5_user' % key]
        action_rank['actions'] += action_rank['%s_if_type6_user' % key]

        action_rank['%s_if_user_rank' % key] = action_rank.rank(ascending=False,method='min')['actions']
        action_rank['%s_if_purchase_user_rank' % key] = action_rank.rank(ascending=False,method='min')['%s_if_type2_user' % key]

        actions = action_rank[features]
        actions.to_csv(dump_path, index=False)
    return actions


# -----------------------------------------------------------------
def get_item_features(start_day, end_day):
    if days_in_record[end_day]-days_in_record[start_day] < 28:
        print "时间太短"
        exit()
    dump_path = '../../cache/item_all_features_%s_%s.csv' % (start_day, end_day)
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
        print "开始计算Item特征"
        i_features = get_basic_item_features()
        print "计数特征计算中"
        print "计算 %s_%s 计数" % (start_day, end_day)
        if_counter = get_item_count_features(start_day,end_day)
        i_features = pd.merge(i_features,if_counter,how='left',on='sku_id')
        # 计算用户的count
        if_counter = get_item_count_user_features(start_day,end_day)
        i_features = pd.merge(i_features,if_counter,how='left',on='sku_id')
        # 计算decay
        if_decayed_counter = get_item_decayed_count_features(start_day,end_day)
        i_features = pd.merge(i_features,if_decayed_counter,how='left',on='sku_id')

        print "3,7,15"
        # action 计算的是额外特征，可以删掉
        if_counter = get_item_count_features(start_15day,end_day)
        i_features = pd.merge(i_features,if_counter,how='left',on='sku_id')
        if_counter = get_item_count_features(start_7day,end_day)
        i_features = pd.merge(i_features,if_counter,how='left',on='sku_id')
        if_counter = get_item_count_features(start_3day,end_day)
        i_features = pd.merge(i_features,if_counter,how='left',on='sku_id')
        # user  计算的是额外特征，可以删掉
        if_counter = get_item_count_user_features(start_15day,end_day)
        i_features = pd.merge(i_features,if_counter,how='left',on='sku_id')
        if_counter = get_item_count_user_features(start_7day,end_day)
        i_features = pd.merge(i_features,if_counter,how='left',on='sku_id')
        if_counter = get_item_count_user_features(start_3day,end_day)
        i_features = pd.merge(i_features,if_counter,how='left',on='sku_id')

        # 活跃度特征
        print "活跃度特征计算中"
        if_active_days = get_item_active_days(start_day,end_day)
        if_active_users = get_item_active_users(start_day,end_day)
        i_features = pd.merge(i_features,if_active_days,how='left',on='sku_id')
        i_features = pd.merge(i_features,if_active_users,how='left',on='sku_id')

        print "3,7"
        if_active_days = get_item_active_days(start_3day,end_day)
        if_active_users = get_item_active_users(start_3day, end_day)
        i_features = pd.merge(i_features,if_active_days,how='left',on='sku_id')
        i_features = pd.merge(i_features,if_active_users,how='left',on='sku_id')

        if_active_days = get_item_active_days(start_7day,end_day)
        if_active_users = get_item_active_users(start_7day, end_day)
        i_features = pd.merge(i_features,if_active_days,how='left',on='sku_id')
        i_features = pd.merge(i_features,if_active_users,how='left',on='sku_id')

        i_features = i_features.fillna(0)

        print "重复购买率"
        if_repeat = get_item_repeat_buy(start_day,end_day)
        print "计算转化率及比值"
        if_trans_next = get_item_trans_features_next(start_day,end_day)
        if_trans = get_item_trans_features(start_day,end_day)
        if_ratio = get_item_ratio_features(start_day,end_day)
        i_features = pd.merge(i_features,if_repeat,how='left',on='sku_id')
        i_features = pd.merge(i_features,if_trans,how='left',on='sku_id')
        i_features = pd.merge(i_features,if_trans_next,how='left',on='sku_id')
        i_features = pd.merge(i_features,if_ratio,how='left',on='sku_id')

        print "计算排名特征"
        if_action_rank = get_item_action_rank_features(start_day,end_day)
        if_user_rank = get_item_user_rank_features(start_day,end_day)
        if_decayed_action_rank = get_item_decayed_action_rank_features(start_day,end_day)
        i_features = pd.merge(i_features,if_action_rank,how='left',on='sku_id')
        i_features = pd.merge(i_features,if_user_rank,how='left',on='sku_id')
        i_features = pd.merge(i_features, if_decayed_action_rank, how='left', on='sku_id')

        print "3,7,15"
        if_action_rank = get_item_action_rank_features(start_3day,end_day)
        if_user_rank = get_item_user_rank_features(start_3day,end_day)
        i_features = pd.merge(i_features,if_action_rank,how='left',on='sku_id')
        i_features = pd.merge(i_features,if_user_rank,how='left',on='sku_id')

        if_action_rank = get_item_action_rank_features(start_7day,end_day)
        if_user_rank = get_item_user_rank_features(start_7day,end_day)
        i_features = pd.merge(i_features,if_action_rank,how='left',on='sku_id')
        i_features = pd.merge(i_features,if_user_rank,how='left',on='sku_id')

        if_action_rank = get_item_action_rank_features(start_15day,end_day)
        if_user_rank = get_item_user_rank_features(start_15day,end_day)
        i_features = pd.merge(i_features,if_action_rank,how='left',on='sku_id')
        i_features = pd.merge(i_features,if_user_rank,how='left',on='sku_id')

        i_features = i_features.fillna(-1)
        actions = i_features
        actions.to_csv(dump_path, index=False)
    return actions


# --------------------------------
# if __name__ == '__main__':
#     start_date = '2016-02-08'
#     end_date = '2016-03-08'
#     # this= get_item_count_features(start_date,end_date)
#     # print this.columns
#     # this = get_item_count_user_features(start_date,end_date)
#     # print this.columns
#     # this = get_item_decayed_count_features(start_date,end_date)
#     # print this.columns
#     # this = get_item_trans_features(start_date,end_date)
#     # print this.columns
#     # this = get_item_trans_features_next(start_date,end_date)
#     # print this.columns
#     # this = get_item_active_days(start_date,end_date)
#     # print this.columns
#     # this = get_item_repeat_buy(start_date,end_date)
#     # print this.columns
#     # this = get_item_ratio_features(start_date,end_date)
#     # print this.columns
#     # this = get_item_action_rank_features(start_date,end_date)
#     # this = get_item_decayed_action_rank_features(start_date,end_date)
#     # this = get_item_user_rank_features(start_date,end_date)
#     this = get_item_features(start_date,end_date)
#     print this.columns

