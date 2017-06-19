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
# cate的非衰减计数特征
def get_cate_count_features(start_day, end_day):
    dump_path = '../../cache/cf_action_count_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        actions = get_actions(start_day, end_day)
        actions = actions[['cate', 'type']]
        df = pd.get_dummies(actions['type'], prefix='action')
        actions = pd.concat([actions, df], axis=1)
        actions = actions.groupby(['cate'], as_index=False).sum()
        del actions['type']
        try:
            actions.rename(columns=lambda x: x.replace('action_1', '%s_cf_type1_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_cf_type1_nums' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_2', '%s_cf_type2_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_cf_type2_nums' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_3', '%s_cf_type3_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_cf_type3_nums' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_4', '%s_cf_type4_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_cf_type4_nums' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_5', '%s_cf_type5_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_cf_type5_nums' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_6', '%s_cf_type6_nums' % key), inplace=True)
        except Exception, e:
            actions['%s_cf_type6_nums' % key] = 0
            print Exception, ":", e

        actions.to_csv(dump_path, index=False)
    return actions


# cate的user技术特征
def get_cate_count_user_features(start_day, end_day):
    dump_path = '../../cache/cf_user_count_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        actions = get_actions(start_day, end_day)
        actions = actions[['cate', 'user_id', 'type']]
        actions = actions.drop_duplicates()
        df = pd.get_dummies(actions['type'], prefix='action')
        actions = pd.concat([actions, df], axis=1)
        del actions['user_id']
        del actions['type']
        actions = actions.groupby(['cate'], as_index=False).sum()
        try:
            actions.rename(columns=lambda x: x.replace('action_1', '%s_cf_type1_user' % key), inplace=True)
        except Exception, e:
            actions['%s_cf_type1_user' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_2', '%s_cf_type2_user' % key), inplace=True)
        except Exception, e:
            actions['%s_cf_type2_user' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_3', '%s_cf_type3_user' % key), inplace=True)
        except Exception, e:
            actions['%s_cf_type3_user' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_4', '%s_cf_type4_user' % key), inplace=True)
        except Exception, e:
            actions['%s_cf_type4_user' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_5', '%s_cf_type5_user' % key), inplace=True)
        except Exception, e:
            actions['%s_cf_type5_user' % key] = 0
            print Exception, ":", e

        try:
            actions.rename(columns=lambda x: x.replace('action_6', '%s_cf_type6_user' % key), inplace=True)
        except Exception, e:
            actions['%s_cf_type6_user' % key] = 0
            print Exception, ":", e

        actions.to_csv(dump_path, index=False)
    return actions


# cate的衰减计数特征
def get_cate_decayed_count_features(start_day, end_day):
    dump_path = '../../cache/cf_action_decayed_count_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        decay = get_decay(range(1,75))
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        actions = get_actions(start_day,end_day)
        counter = actions.copy()[['cate','time','type']]
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
        counter = counter.groupby(['cate'], as_index=False).sum()

        try:
            counter.rename(columns=lambda x: x.replace('action_1', '%s_cf_type1_dnums' % key), inplace=True)
        except Exception, e:
            counter['%s_cf_type1_dnums' % key] = 0
            print Exception, ":", e

        try:
            counter.rename(columns=lambda x: x.replace('action_2', '%s_cf_type2_dnums' % key), inplace=True)
        except Exception, e:
            counter['%s_cf_type2_dnums' % key] = 0
            print Exception, ":", e

        try:
            counter.rename(columns=lambda x: x.replace('action_3', '%s_cf_type3_dnums' % key), inplace=True)
        except Exception, e:
            counter['%s_cf_type3_dnums' % key] = 0
            print Exception, ":", e

        try:
            counter.rename(columns=lambda x: x.replace('action_4', '%s_cf_type4_dnums' % key), inplace=True)
        except Exception, e:
            counter['%s_cf_type4_dnums' % key] = 0
            print Exception, ":", e

        try:
            counter.rename(columns=lambda x: x.replace('action_5', '%s_cf_type5_dnums' % key), inplace=True)
        except Exception, e:
            counter['%s_cf_type5_dnums' % key] = 0
            print Exception, ":", e

        try:
            counter.rename(columns=lambda x: x.replace('action_6', '%s_cf_type6_dnums' % key), inplace=True)
        except Exception, e:
            counter['%s_cf_type6_dnums' % key] = 0
            print Exception, ":", e

        actions = counter
        actions.to_csv(dump_path, index=False)
    return actions


# -----------------------------------------------------------------
# cate的比值特征
def get_cate_ratio_features(start_day, end_day):
    features = ['cate', 'cf_type1_ratio', 'cf_type2_ratio', 'cf_type3_ratio','cf_type5_ratio','cf_type6_ratio'
                , 'cf_type1_user_ratio', 'cf_type2_user_ratio', 'cf_type3_user_ratio','cf_type5_user_ratio','cf_type6_user_ratio']
    dump_path = '../../cache/cf_action_ratio_global.csv'
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        # 计算用户的转化率
        actions = get_cate_count_user_features(start_day,end_day)
        actions['cf_type1_user_ratio'] = actions['%s_cf_type4_user' % key] / actions['%s_cf_type1_user' % key]
        actions['cf_type2_user_ratio'] = actions['%s_cf_type4_user' % key] / actions['%s_cf_type2_user' % key]
        actions['cf_type3_user_ratio'] = actions['%s_cf_type4_user' % key] / actions['%s_cf_type3_user' % key]
        actions['cf_type5_user_ratio'] = actions['%s_cf_type4_user' % key] / actions['%s_cf_type5_user' % key]
        actions['cf_type6_user_ratio'] = actions['%s_cf_type4_user' % key] / actions['%s_cf_type6_user' % key]
        actions_nums = get_cate_count_features(start_day,end_day)
        # 计算操作的转化率
        actions_nums['cf_type1_ratio'] = actions_nums['%s_cf_type4_nums' % key] / actions_nums['%s_cf_type1_nums' % key]
        actions_nums['cf_type2_ratio'] = actions_nums['%s_cf_type4_nums' % key] / actions_nums['%s_cf_type2_nums' % key]
        actions_nums['cf_type3_ratio'] = actions_nums['%s_cf_type4_nums' % key] / actions_nums['%s_cf_type3_nums' % key]
        actions_nums['cf_type5_ratio'] = actions_nums['%s_cf_type4_nums' % key] / actions_nums['%s_cf_type5_nums' % key]
        actions_nums['cf_type6_ratio'] = actions_nums['%s_cf_type4_nums' % key] / actions_nums['%s_cf_type6_nums' % key]

        actions = pd.merge(actions, actions_nums, how='left', on=['cate'])
        actions = actions[features]
        actions.to_csv(dump_path, index=False)
    return actions


# cate的转化率
def get_cate_trans_features(start_day, end_day):
    dump_path = '../../cache/cf_action_trans_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions = get_actions(start_day,end_day)
        actions_trans = actions[['user_id', 'sku_id', 'type', 'cate']]
        # 去掉重复type，我只需要知道用户对商品进行过哪些操作
        actions_trans = actions_trans.drop_duplicates()
        df = pd.get_dummies(actions_trans['type'], prefix='action')
        actions_trans = pd.concat([actions_trans, df], axis=1)
        del actions_trans['type']
        # 统计一个表，每一行就可以知道这件商品进行过哪些操作
        actions_trans = actions_trans.groupby(['cate','user_id', 'sku_id'],as_index=False).sum()
        # 找到操作不为0的子表去找他们对于购买的转化率
        actions_trans_type1 = actions_trans[actions_trans['action_1'] > 0]
        actions_trans_type2 = actions_trans[actions_trans['action_2'] > 0]
        actions_trans_type3 = actions_trans[actions_trans['action_3'] > 0]
        actions_trans_type5 = actions_trans[actions_trans['action_5'] > 0]
        actions_trans_type6 = actions_trans[actions_trans['action_6'] > 0]
        # 计算cate所有操作过的商品的转化率特征
        actions_trans_type1 = actions_trans_type1.groupby('cate',as_index=False).sum()
        actions_trans_type2 = actions_trans_type2.groupby('cate',as_index=False).sum()
        actions_trans_type3 = actions_trans_type3.groupby('cate',as_index=False).sum()
        actions_trans_type5 = actions_trans_type5.groupby('cate',as_index=False).sum()
        actions_trans_type6 = actions_trans_type6.groupby('cate',as_index=False).sum()
        actions_trans_type1['cf_type1_trans'] = actions_trans_type1['action_4']/actions_trans_type1['action_1']
        actions_trans_type2['cf_type2_trans'] = actions_trans_type2['action_4']/actions_trans_type2['action_2']
        actions_trans_type3['cf_type3_trans'] = actions_trans_type3['action_4']/actions_trans_type3['action_3']
        actions_trans_type5['cf_type5_trans'] = actions_trans_type5['action_4']/actions_trans_type5['action_5']
        actions_trans_type6['cf_type6_trans'] = actions_trans_type6['action_4']/actions_trans_type6['action_6']
        actions_trans_type1 = actions_trans_type1[['cate','cf_type1_trans']]
        actions_trans_type2 = actions_trans_type2[['cate','cf_type2_trans']]
        actions_trans_type3 = actions_trans_type3[['cate','cf_type3_trans']]
        actions_trans_type5 = actions_trans_type5[['cate','cf_type5_trans']]
        actions_trans_type6 = actions_trans_type6[['cate','cf_type6_trans']]
        actions_trans = actions_trans[['cate']]
        actions_trans = actions_trans.drop_duplicates()
        actions_trans = pd.merge(actions_trans, actions_trans_type1, how='left', on=['cate'])
        actions_trans = pd.merge(actions_trans, actions_trans_type2, how='left', on=['cate'])
        actions_trans = pd.merge(actions_trans, actions_trans_type3, how='left', on=['cate'])
        actions_trans = pd.merge(actions_trans, actions_trans_type5, how='left', on=['cate'])
        actions_trans = pd.merge(actions_trans, actions_trans_type6, how='left', on=['cate'])
        actions = actions_trans
        actions.to_csv(dump_path, index=False)
    return actions


# cate的隔天转化率
def get_cate_trans_features_next(start_day, end_day):
    dump_path = '../../cache/cf_action_trans_next_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions = get_actions(start_day,end_day)
        actions['time'] = actions.loc[:, 'time'].apply(lambda x: days_in_record[x[0:10]])
        actions_ratio = actions[['cate', 'user_id', 'sku_id', 'time', 'type']]
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
                                            on=['cate', 'user_id', 'sku_id', 'time'])
        # 如果flag是1说明type2之后，第二天买了，否则是0
        actions_ratio_type2_next = actions_ratio_type2_next[['cate', 'flag']]
        # 补0
        actions_ratio_type2_next = actions_ratio_type2_next.fillna(0)
        # 对于用户所有type2的商品，看看flag的平均值是多少
        actions_type2_ratio = actions_ratio_type2_next.groupby('cate', as_index=False).mean()
        # 结果保存在type2——ratio中
        actions_type2_ratio.rename(columns=lambda x: x.replace('flag', 'cf_type2_trans_next'), inplace=True)

        actions_ratio_type5_next = actions_ratio_type5.copy()
        actions_ratio_type5_next = pd.merge(actions_ratio_type5_next, actions_ratio_type4_forward, how='left',
                                            on=['cate','user_id', 'sku_id', 'time'])
        actions_ratio_type5_next = actions_ratio_type5_next[['cate', 'flag']]
        actions_ratio_type5_next = actions_ratio_type5_next.fillna(0)
        # 这个特征比较稀疏
        actions_type5_ratio = actions_ratio_type5_next.groupby('cate', as_index=False).mean()
        actions_type5_ratio.rename(columns=lambda x: x.replace('flag', 'cf_type5_trans_next'), inplace=True)
        actions_ratio = actions[['cate']].drop_duplicates()
        actions_ratio = pd.merge(actions_ratio, actions_type2_ratio, how='left', on=['cate'])
        actions_ratio = pd.merge(actions_ratio, actions_type5_ratio, how='left', on=['cate'])
        actions = actions_ratio
        actions.to_csv(dump_path, index=False)
    return actions


# -----------------------------------------------------------------
# cate重复购买
def get_cate_repeat_buy(start_day, end_day):
    dump_path = '../../cache/cf_repeat_buy_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions = get_actions(start_day,end_day)
        actions_repeat = actions[['user_id', 'cate', 'type','sku_id']]
        # 找出所有的购买
        actions_repeat = actions_repeat[actions_repeat['type'] == 4]
        del actions_repeat['type']
        # 计算所有的用户在cate上购买的数量
        actions_repeat_count = actions_repeat.groupby(['cate', 'user_id'], as_index=False).count()
        del actions_repeat_count['user_id']
        # 找出重复购买
        actions_repeat_count = actions_repeat_count[actions_repeat_count['sku_id'] > 1]
        # 计算出重复购买的所有购买数
        actions_repeat_count = actions_repeat_count.groupby('cate',as_index=False).sum()
        actions_repeat = actions_repeat[['cate','user_id']]
        # 计算出所有cate的购买数量
        actions_repeat = actions_repeat.groupby('cate',as_index=False).count()
        actions_repeat = pd.merge(actions_repeat,actions_repeat_count,how='left',on=['cate'])
        # 我们用所有重复购买的数量（大于一次的所有购买的和）去除以所有购买，看重复购买的比例
        actions_repeat['cf_repeat_buys'] = actions_repeat['sku_id'] / actions_repeat['user_id']
        actions = actions_repeat[['cate', 'cf_repeat_buys']]
        actions.to_csv(dump_path, index=False)
    return actions


# cate交互过的用户数
def get_cate_active_users(start_day, end_day):
    dump_path = '../../cache/cf_active_users_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        actions = get_actions(start_day,end_day)
        active_cates = actions.copy()[['user_id','cate']]
        active_cates = active_cates.drop_duplicates()
        active_cates = active_cates.groupby(['cate'], as_index=False).count()
        actions = active_cates.loc[:, ['user_id', 'cate']]
        actions.rename(columns=lambda x: x.replace('user_id', '%s_cf_active_users' % key), inplace=True)
        actions.to_csv(dump_path, index=False)
    return actions


# -----------------------------------------------------------------
# 计算cate的衰减排名特征
def get_cate_decayed_action_rank_features(start_day, end_day):
    dump_path = '../../cache/cf_decayed_action_rank_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        features = ['cate', '%s_cf_decayed_action_rank' % key, '%s_cf_decayed_purchase_rank' % key]

        action_rank = get_cate_decayed_count_features(start_day,end_day)
        action_rank['actions'] = 0
        action_rank['actions'] += action_rank['%s_cf_type1_dnums' % key]
        action_rank['actions'] += action_rank['%s_cf_type2_dnums' % key]
        action_rank['actions'] += action_rank['%s_cf_type3_dnums' % key]
        action_rank['actions'] += action_rank['%s_cf_type4_dnums' % key]
        action_rank['actions'] += action_rank['%s_cf_type5_dnums' % key]
        action_rank['actions'] += action_rank['%s_cf_type6_dnums' % key]

        action_rank['%s_cf_decayed_action_rank' % key] = action_rank.rank(ascending=False,method='min')['actions']
        action_rank['%s_cf_decayed_purchase_rank' % key] = action_rank.rank(ascending=False,method='min')['%s_cf_type2_dnums' % key]

        actions = action_rank[features]
        actions.to_csv(dump_path, index=False)
    return actions


# -----------------------------------------------------------------
def get_cate_features(start_day, end_day):
    if days_in_record[end_day]-days_in_record[start_day] < 28:
        print "时间太短"
        exit()
    dump_path = '../../cache/cate_all_features_%s_%s.csv' % (start_day, end_day)
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
        print "开始计算cate特征"
        print "计数特征计算中"
        print "计算 %s_%s 计数" % (start_day, end_day)
        cf_counter = get_cate_count_features(start_day,end_day)
        c_features = cf_counter
        # 计算用户的count
        cf_counter = get_cate_count_user_features(start_day,end_day)
        c_features = pd.merge(c_features,cf_counter,how='left',on='cate')
        # 计算decay
        cf_decayed_counter = get_cate_decayed_count_features(start_day,end_day)
        c_features = pd.merge(c_features,cf_decayed_counter,how='left',on='cate')

        print "3,7,15"
        # action 计算的是额外特征，可以删掉
        cf_counter = get_cate_count_features(start_15day,end_day)
        c_features = pd.merge(c_features,cf_counter,how='left',on='cate')
        cf_counter = get_cate_count_features(start_7day,end_day)
        c_features = pd.merge(c_features,cf_counter,how='left',on='cate')
        cf_counter = get_cate_count_features(start_3day,end_day)
        c_features = pd.merge(c_features,cf_counter,how='left',on='cate')
        # user  计算的是额外特征，可以删掉
        cf_counter = get_cate_count_user_features(start_15day,end_day)
        c_features = pd.merge(c_features,cf_counter,how='left',on='cate')
        cf_counter = get_cate_count_user_features(start_7day,end_day)
        c_features = pd.merge(c_features,cf_counter,how='left',on='cate')
        cf_counter = get_cate_count_user_features(start_3day,end_day)
        c_features = pd.merge(c_features,cf_counter,how='left',on='cate')

        # 活跃度特征
        print "活跃度特征计算中"
        cf_active_users = get_cate_active_users(start_day,end_day)
        c_features = pd.merge(c_features,cf_active_users,how='left',on='cate')

        print "3,7"
        cf_active_users = get_cate_active_users(start_3day, end_day)
        c_features = pd.merge(c_features,cf_active_users,how='left',on='cate')

        cf_active_users = get_cate_active_users(start_7day, end_day)
        c_features = pd.merge(c_features,cf_active_users,how='left',on='cate')

        c_features = c_features.fillna(0)

        print "重复购买率"
        cf_repeat = get_cate_repeat_buy(start_day,end_day)
        print "计算转化率及比值"
        cf_trans_next = get_cate_trans_features_next(start_day,end_day)
        cf_trans = get_cate_trans_features(start_day,end_day)
        cf_ratio = get_cate_ratio_features(start_day,end_day)
        c_features = pd.merge(c_features,cf_repeat,how='left',on='cate')
        c_features = pd.merge(c_features,cf_trans,how='left',on='cate')
        c_features = pd.merge(c_features,cf_trans_next,how='left',on='cate')
        c_features = pd.merge(c_features,cf_ratio,how='left',on='cate')

        print "计算排名特征"
        cf_decayed_action_rank = get_cate_decayed_action_rank_features(start_day,end_day)
        c_features = pd.merge(c_features, cf_decayed_action_rank, how='left', on='cate')

        c_features = c_features.fillna(-1)
        actions = c_features
        actions.to_csv(dump_path, index=False)
    return actions


# -----------------------------------------------------------------
# if __name__ == '__main__':
#     print 'begin'
#     start_date = '2016-02-08'
#     end_date = '2016-03-08'
#     # this = get_cate_count_features(start_date,end_date)
#     # this = get_cate_count_user_features(start_date,end_date)
#     # this = get_cate_decayed_count_features(start_date,end_date)
#     # this = get_cate_ratio_features(start_date,end_date)
#     # this = get_cate_trans_features(start_date,end_date)
#     # this = get_cate_trans_features_next(start_date,end_date)
#     # this = get_cate_repeat_buy(start_date,end_date)
#     # this = get_cate_active_days(start_date,end_date)
#     # this = get_cate_active_users(start_date,end_date)
#     # this = get_cate_action_rank_features(start_date,end_date)
#     # this = get_cate_decayed_action_rank_features(start_date, end_date)
#     # this = get_cate_action_rank_features(start_date,end_date)
#     # print this.columns
#     # this = get_cate_user_rank_features(start_date,end_date)
#     # print this.columns
#     # this = get_cate_decayed_action_rank_features(start_date,end_date)
#     # print this.columns
#     get_cate_features(start_date,end_date)