# coding=utf-8
import pandas as pd
import os
import datetime
import time
import math
import numpy as np
from data_path import *
from get_actions import get_all_actions, get_actions


# **对数衰减
def get_decay(days):
    first = [1]
    decay = range(1,len(days)+1)
    counter = 0
    for i in days:
        decay[counter] = 0.7**(2*math.log(i) + 1)
        counter +=1
    return first+decay


# 计算基本的type数量 计数特征（basic）
# **计算3-6日会有问题
def get_ub_basic_count(start_day, end_day):
    features = ['user_id', 'brand','%s_%s_ub_action_1' % (start_day, end_day),'%s_%s_ub_action_2' % (start_day, end_day),
                '%s_%s_ub_action_3' % (start_day, end_day),'%s_%s_ub_action_4' % (start_day, end_day),
                '%s_%s_ub_action_5' % (start_day, end_day),'%s_%s_ub_action_6' % (start_day, end_day)]
    dump_path = '../../cache/ubf_basic_count_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions_day = get_actions(start_day, end_day)
        actions_day = actions_day[['user_id', 'brand', 'type']]
        df = pd.get_dummies(actions_day['type'], prefix='%s_%s_ub_action' % (start_day, end_day))
        actions_day = pd.concat([actions_day, df], axis=1)
        actions_day = actions_day.groupby(['user_id', 'brand'], as_index=False).sum()
        del actions_day['type']
        # 防止有的天没有某种动作
        try:
            actions_day['%s_%s_ub_action_2' % (start_day, end_day)]
        except Exception, e:
            actions_day['%s_%s_ub_action_2' % (start_day, end_day)] = 0
            print Exception, ":", e

        try:
            actions_day['%s_%s_ub_action_3' % (start_day, end_day)]
        except Exception, e:
            actions_day['%s_%s_ub_action_3' % (start_day, end_day)] = 0
            print Exception, ":", e
        try:
            actions_day['%s_%s_ub_action_4' % (start_day, end_day)]
        except Exception, e:
            actions_day['%s_%s_ub_action_4' % (start_day, end_day)] = 0
            print Exception, ":", e
        try:
            actions_day['%s_%s_ub_action_5' % (start_day, end_day)]
        except Exception, e:
            actions_day['%s_%s_ub_action_5' % (start_day, end_day)] = 0
            print Exception, ":", e

        actions = actions_day[features]
        actions.to_csv(dump_path, index=False)
    return actions


""" 在这里删除了对于type的比例特征
    在UC聚合上感觉用处不大
"""


# **计算用户当天在该brand上的操作数 计数特征（basic）
def get_ub_basic_all_type_count(start_day, end_day):
    dump_path = '../../cache/ubf_basic_all_count_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions_all_type_nums = get_ub_basic_count(start_day, end_day)
        actions_all_type_nums['%s_%s_ubf_all_nums' % (start_day, end_day)] \
            = actions_all_type_nums['%s_%s_ub_action_1' % (start_day, end_day)] + \
              actions_all_type_nums['%s_%s_ub_action_2' % (start_day, end_day)] + \
              actions_all_type_nums['%s_%s_ub_action_3' % (start_day, end_day)] + \
              actions_all_type_nums['%s_%s_ub_action_4' % (start_day, end_day)] + \
              actions_all_type_nums['%s_%s_ub_action_5' % (start_day, end_day)] + \
              actions_all_type_nums['%s_%s_ub_action_6' % (start_day, end_day)]
        actions = actions_all_type_nums[['user_id','brand','%s_%s_ubf_all_nums' % (start_day, end_day)]]
        actions.to_csv(dump_path, index=False)
    return actions


# **计算用户当天对该商品操作的占比
def get_ub_basic_action_ratio(start_day, end_day):
    dump_path = '../../cache/ubf_basic_action_ratio_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions_all_type = get_ub_basic_all_type_count(start_day,end_day)
        actions_all = get_actions(start_day,end_day)
        actions_ratio = actions_all[['user_id','brand']]
        actions_ratio = actions_ratio.groupby('user_id',as_index=False).count()
        actions_ratio.rename(columns=lambda x: x.replace('brand', 'total_nums'), inplace=True)
        actions_ratio = pd.merge(actions_all_type,actions_ratio,how='left',on=['user_id'])
        actions_ratio['%s_%s_ubf_action_ration'% (start_day, end_day)] = \
            actions_ratio['%s_%s_ubf_all_nums' % (start_day, end_day)]/actions_ratio['total_nums']
        actions = actions_ratio[['user_id','brand','%s_%s_ubf_action_ration'% (start_day, end_day)]]
        actions.to_csv(dump_path, index=False)
    return actions


# -----------------------------------------
# XX计数特征 days 还有 hours
def get_ub_type_day_hour_count(end_day, days=range(1, 5)):
    days.sort()
    dump_path = '../../cache/ubf_type_count_%s.csv' % end_day
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        datetime_end_date = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        actions_type_count = None
        # 生成具体天
        for i in days:
            datetime_start_date = datetime_end_date - datetime.timedelta(days=i)
            start_day = datetime_start_date.strftime('%Y-%m-%d')
            print start_day, end_day
            action_unit = get_ub_basic_count(start_day, end_day)
            end_day = start_day
            if actions_type_count is None:
                actions_type_count = action_unit
            else:
                actions_type_count = pd.merge(actions_type_count, action_unit, how='outer', on=['user_id', 'brand'])
        actions = actions_type_count
        actions.to_csv(dump_path, index=False)
    return actions


# XX计数特征 acc ub_acc_days 的
def get_ub_type_accdays_count(end_day,accdays=[3, 5, 7, 10, 15]):
    accdays.sort(reverse=True)
    features=['user_id','brand']
    for i in accdays:
        features.append('%s_ub_action1' %i)
        features.append('%s_ub_action2' %i)
        features.append('%s_ub_action3' %i)
        features.append('%s_ub_action4' %i)
        features.append('%s_ub_action5' %i)
        features.append('%s_ub_action6' %i)
    glo_end = end_day
    dump_path = '../../cache/ubf_type_acc_count_%s.csv' % end_day
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        # 合成一张大表
        peroid = max(accdays)
        decay = get_decay(range(1,peroid))
        day_count = None
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        for this_day in range(1,peroid+1):
            dt_start_day = dt_end_day - datetime.timedelta(days=1)
            start_day = dt_start_day.strftime('%Y-%m-%d')
            end_day = dt_end_day.strftime('%Y-%m-%d')
            print start_day, end_day, this_day
            dt_end_day = dt_start_day
            this = get_ub_basic_count(start_day, end_day)
            if day_count is None:
                day_count = this
            else:
                day_count = pd.merge(day_count, this, how='outer', on=['user_id', 'brand'])
        day_count = day_count.fillna(0)
        for this_peroid in accdays:
            dt_end_day = datetime.datetime.strptime(glo_end, '%Y-%m-%d')
            day_count['%s_ub_action1' % this_peroid] = 0
            day_count['%s_ub_action2' % this_peroid] = 0
            day_count['%s_ub_action3' % this_peroid] = 0
            day_count['%s_ub_action4' % this_peroid] = 0
            day_count['%s_ub_action5' % this_peroid] = 0
            day_count['%s_ub_action6' % this_peroid] = 0
            for this_day in range(1, this_peroid + 1):
                dt_start_day = dt_end_day - datetime.timedelta(days=1)
                start_day = dt_start_day.strftime('%Y-%m-%d')
                end_day = dt_end_day.strftime('%Y-%m-%d')
                print start_day, end_day, this_day,decay[this_day-1]
                dt_end_day = dt_start_day
                day_count['%s_ub_action1' % this_peroid] += (day_count['%s_%s_ub_action_1' % (start_day,end_day)] * decay[this_day-1])
                day_count['%s_ub_action2' % this_peroid] += (day_count['%s_%s_ub_action_2' % (start_day,end_day)] * decay[this_day-1])
                day_count['%s_ub_action3' % this_peroid] += (day_count['%s_%s_ub_action_3' % (start_day,end_day)] * decay[this_day-1])
                day_count['%s_ub_action4' % this_peroid] += (day_count['%s_%s_ub_action_4' % (start_day,end_day)] * decay[this_day-1])
                day_count['%s_ub_action5' % this_peroid] += (day_count['%s_%s_ub_action_5' % (start_day,end_day)] * decay[this_day-1])
                day_count['%s_ub_action6' % this_peroid] += (day_count['%s_%s_ub_action_6' % (start_day,end_day)] * decay[this_day-1])
        actions = day_count[features]
        actions.to_csv(dump_path, index=False)
    return actions


# XX比例特征 显著性特征
def get_ub_action_ratio_day_hour(end_day, days=range(1, 5)):
    days.sort()
    dump_path = '../../cache/ubf_action_ratio_day_hour_%s.csv' % end_day
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        datetime_end_date = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        actions_ratio = None
        # 生成具体天
        for i in days:
            datetime_start_date = datetime_end_date - datetime.timedelta(days=i)
            start_day = datetime_start_date.strftime('%Y-%m-%d')
            print start_day, end_day, i
            action_unit = get_ub_basic_action_ratio(start_day, end_day)
            end_day = start_day
            if actions_ratio is None:
                actions_ratio = action_unit
            else:
                actions_ratio = pd.merge(actions_ratio, action_unit, how='outer', on=['user_id', 'brand'])
        actions = actions_ratio
        actions.to_csv(dump_path, index=False)
    return actions


# XX比例特征，显著性acc特征
def get_ub_action_accdays_ratio(end_day,accdays=[3, 5, 7, 10, 15]):
    accdays.sort(reverse=True)
    features = ['user_id', 'brand']
    for i in accdays:
        features.append('%s_ub_action_ratio' % i)
    glo_end = end_day
    dump_path = '../../cache/ubf_action_acc_ratio_%s.csv' % end_day
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        # 合成一张大表，内容为每天的操作数总和
        peroid = max(accdays)
        decay = get_decay(range(1, peroid))
        day_count = None
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        for this_day in range(1, peroid + 1):
            dt_start_day = dt_end_day - datetime.timedelta(days=1)
            start_day = dt_start_day.strftime('%Y-%m-%d')
            end_day = dt_end_day.strftime('%Y-%m-%d')
            print start_day, end_day, this_day
            dt_end_day = dt_start_day
            this = get_ub_basic_all_type_count(start_day, end_day)
            if day_count is None:
                day_count = this
            else:
                day_count = pd.merge(day_count, this, how='outer', on=['user_id', 'brand'])
        day_count = day_count.fillna(0)
        for this_peroid in accdays:
            dt_end_day = datetime.datetime.strptime(glo_end, '%Y-%m-%d')
            day_count['%s_action_nums' % this_peroid] = 0
            for this_day in range(1, this_peroid + 1):
                dt_start_day = dt_end_day - datetime.timedelta(days=1)
                start_day = dt_start_day.strftime('%Y-%m-%d')
                end_day = dt_end_day.strftime('%Y-%m-%d')
                print start_day, end_day, this_day, decay[this_day - 1]
                dt_end_day = dt_start_day
                day_count['%s_action_nums' % this_peroid] += \
                    (day_count['%s_%s_ubf_all_nums' % (start_day, end_day)] * decay[this_day - 1])
        left_columns = ['user_id','brand']
        for i in accdays:
            left_columns.append('%s_action_nums' % i)
        day_count = day_count[left_columns]
        # 计算用户操作的总数目
        for this_columns in accdays:
            day_all = day_count.groupby(['user_id'],as_index=False).agg({'%s_action_nums' % this_columns:'sum'})
            day_all.rename(columns=lambda x: x.replace('%s_action_nums' % this_columns, '%s_user_action_nums' % this_columns), inplace=True)
            day_count = pd.merge(day_count,day_all,how='left',on='user_id')
        for this_peroid in accdays:
            day_count['%s_ub_action_ratio' % this_peroid] = day_count['%s_action_nums' % this_peroid] \
                                                         / day_count['%s_user_action_nums' % this_peroid]
        actions = day_count[features]
        actions.to_csv(dump_path, index=False)
    return actions


# --------------------------------------------
# $$ 计算用户在该商品上的活跃天数
def get_ub_active_days(start_day, end_day):
    dump_path = '../../cache/ubf_active_days_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions = get_actions(start_day,end_day)
        active_days = actions.copy()[['user_id','brand','time']]
        active_days['time'] = active_days.loc[:, 'time'].apply(lambda x: x[0:10])
        active_days = active_days.drop_duplicates()
        active_days = active_days.groupby(['user_id','brand'],as_index=False).count()
        active_days.rename(columns=lambda x: x.replace('time', '%s_%s_ubf_active_days' % (start_day, end_day)), inplace=True)
        actions = active_days
        actions.to_csv(dump_path, index=False)
    return actions


# --------------------------------------------
# 排名特征
# 这个也需要按时间段去统计
# 这个只能计算当天的排名特征
def get_ub_acc_counter_rank(end_day,accdays=[3, 5, 7, 10, 15]):
    features = ['user_id','brand']
    for i in accdays:
        # print i
        features.append('ubf_action_rank_%s' %i)
        features.append('ubf_type2_rank_%s' %i)
        features.append('ubf_type3_rank_%s' % i)
        features.append('ubf_type5_rank_%s' % i)
    dump_path = '../../cache/ub_acc_counter_rank_%s.csv' % end_day
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        acc_rank = get_ub_type_accdays_count(end_day)
        for this_day in accdays:
            # print this_day
            acc_rank['%s_actions' % this_day] = 0
            acc_rank['%s_actions' % this_day] += acc_rank['%s_ub_action1' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_ub_action2' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_ub_action3' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_ub_action4' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_ub_action5' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_ub_action6' % this_day]
        for this_day in accdays:
            acc_rank['ubf_action_rank_%s' % this_day] = acc_rank.rank(ascending=False,method='min')['%s_actions' % this_day]
            acc_rank['ubf_type2_rank_%s' % this_day] = acc_rank.rank(ascending=False,method='min')['%s_ub_action2' % this_day]
            acc_rank['ubf_type3_rank_%s' % this_day] = acc_rank.rank(ascending=False,method='min')['%s_ub_action3' % this_day]
            acc_rank['ubf_type5_rank_%s' % this_day] = acc_rank.rank(ascending=False,method='min')['%s_ub_action5' % this_day]
        actions = acc_rank[features]
        actions.to_csv(dump_path, index=False)
    return actions


# -------------------
def get_ub_features(start_day,end_day):
    dump_path = '../../cache/ub_all_features_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        # 计数特征
        # accdays
        print "开始计算ub特征"
        print "计数特征计算中~~~~"
        ubf_counter_acc = get_ub_type_accdays_count(end_day)
        ub_features = ubf_counter_acc
        # days & hours
        ubf_counter_hour_day = get_ub_type_day_hour_count(end_day)
        ub_features = pd.merge(ub_features,ubf_counter_hour_day,how='left',on=['user_id','brand'])

        # 比例特征
        print "比例特征计算中~~~"

        # 操作显著性特征
        ubf_action_ration = get_ub_action_ratio_day_hour(end_day)
        ub_features = pd.merge(ub_features,ubf_action_ration,how='left',on=['user_id','brand'])
        ubf_action_ration_acc = get_ub_action_accdays_ratio(end_day)
        ub_features = pd.merge(ub_features,ubf_action_ration_acc,how='left',on=['user_id','brand'])

        # 对于前面的特征，存在0/0的可能，直接补零
        ub_features = ub_features.fillna(0)
        # 活跃度特征
        print "活跃度特征&时间特征~~~"
        ubf_active_days = get_ub_active_days(start_day, end_day)
        ub_features = pd.merge(ub_features,ubf_active_days,how='left',on=['user_id','brand'])

        # 排名特征
        print "排名特征~~~"
        print "累积排名"
        ubf_ub_rank = get_ub_acc_counter_rank(end_day)
        ub_features = pd.merge(ub_features,ubf_ub_rank,how='left',on=['user_id','brand'])

        # -1表示未知
        ub_features = ub_features.fillna(-1)
        print "ub特征完结 撒花！！！"
        actions = ub_features
        actions.to_csv(dump_path, index=False)
    return actions


# if __name__ == '__main__':
    start_date = '2016-03-06'
    end_date = '2016-03-07'
    start = time.clock()
    get_ub_features('2016-02-01','2016-03-01')
#     # get_ub_features('2016-03-11', '2016-04-11')
#     # get_ub_acc_counter_rank('2016-04-11')
    end = time.clock()
    print("The function run time is : %.03f seconds" % (end - start))