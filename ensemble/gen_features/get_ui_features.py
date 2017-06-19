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


# 计算基本的type数量 计数特征（basic）
# **计算3-6日会有问题
def get_ui_basic_count(start_day, end_day):
    features = ['user_id', 'sku_id','%s_%s_action_1' % (start_day, end_day),'%s_%s_action_2' % (start_day, end_day),
                '%s_%s_action_3' % (start_day, end_day),'%s_%s_action_4' % (start_day, end_day),
                '%s_%s_action_5' % (start_day, end_day),'%s_%s_action_6' % (start_day, end_day)]
    dump_path = '../../cache/uif_basic_count_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions_day = get_actions(start_day, end_day)
        actions_day = actions_day[['user_id', 'sku_id', 'type']]
        df = pd.get_dummies(actions_day['type'], prefix='%s_%s_action' % (start_day, end_day))
        actions_day = pd.concat([actions_day, df], axis=1)
        actions_day = actions_day.groupby(['user_id', 'sku_id'], as_index=False).sum()
        del actions_day['type']
        # 防止有的天没有某种动作
        try:
            actions_day['%s_%s_action_2' % (start_day, end_day)]
        except Exception, e:
            actions_day['%s_%s_action_2' % (start_day, end_day)] = 0
            print Exception, ":", e

        try:
            actions_day['%s_%s_action_3' % (start_day, end_day)]
        except Exception, e:
            actions_day['%s_%s_action_3' % (start_day, end_day)] = 0
            print Exception, ":", e
        try:
            actions_day['%s_%s_action_4' % (start_day, end_day)]
        except Exception, e:
            actions_day['%s_%s_action_4' % (start_day, end_day)] = 0
            print Exception, ":", e
        try:
            actions_day['%s_%s_action_5' % (start_day, end_day)]
        except Exception, e:
            actions_day['%s_%s_action_5' % (start_day, end_day)] = 0
            print Exception, ":", e

        actions = actions_day[features]
        actions.to_csv(dump_path, index=False)
    return actions


# **计算用户当天在该sku上的操作数 计数特征（basic）
def get_ui_basic_all_type_count(start_day, end_day):
    dump_path = '../../cache/uif_basic_all_count_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions_all_type_nums = get_ui_basic_count(start_day, end_day)
        actions_all_type_nums['%s_%s_uif_all_nums' % (start_day, end_day)] \
            = actions_all_type_nums['%s_%s_action_1' % (start_day, end_day)] + \
              actions_all_type_nums['%s_%s_action_2' % (start_day, end_day)] + \
              actions_all_type_nums['%s_%s_action_3' % (start_day, end_day)] + \
              actions_all_type_nums['%s_%s_action_4' % (start_day, end_day)] + \
              actions_all_type_nums['%s_%s_action_5' % (start_day, end_day)] + \
              actions_all_type_nums['%s_%s_action_6' % (start_day, end_day)]
        actions = actions_all_type_nums[['user_id','sku_id','%s_%s_uif_all_nums' % (start_day, end_day)]]
        actions.to_csv(dump_path, index=False)
    return actions


# **计算用户当天该type的占比 比例特征（basic）
def get_ui_basic_type_ratio(start_day, end_day):
    features = ['user_id','sku_id','%s_%s_uif_type1_ratio' % (start_day, end_day),'%s_%s_uif_type2_ratio' % (start_day, end_day),
                '%s_%s_uif_type3_ratio' % (start_day, end_day),'%s_%s_uif_type4_ratio' % (start_day, end_day),
                '%s_%s_uif_type5_ratio' % (start_day, end_day),'%s_%s_uif_type6_ratio' % (start_day, end_day)]
    dump_path = '../../cache/uif_basic_type_ratio_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions_type_nums = get_ui_basic_count(start_day, end_day)
        actions_all_type_nums = get_ui_basic_all_type_count(start_day, end_day)
        actions_type_ratio = pd.merge(actions_all_type_nums,actions_type_nums,how='left',on=['user_id','sku_id'])

        actions_type_ratio['%s_%s_uif_type1_ratio' % (start_day, end_day)] = actions_type_ratio['%s_%s_action_1' %
         (start_day, end_day)] / actions_type_ratio['%s_%s_uif_all_nums' % (start_day, end_day)]

        actions_type_ratio['%s_%s_uif_type2_ratio' % (start_day, end_day)] = actions_type_ratio['%s_%s_action_2' %
         (start_day, end_day)] / actions_type_ratio['%s_%s_uif_all_nums' % (start_day, end_day)]

        actions_type_ratio['%s_%s_uif_type3_ratio' % (start_day, end_day)] = actions_type_ratio['%s_%s_action_3' %
         (start_day, end_day)] / actions_type_ratio['%s_%s_uif_all_nums' % (start_day, end_day)]

        actions_type_ratio['%s_%s_uif_type4_ratio' % (start_day, end_day)] = actions_type_ratio['%s_%s_action_4' %
         (start_day, end_day)] / actions_type_ratio['%s_%s_uif_all_nums' % (start_day, end_day)]

        actions_type_ratio['%s_%s_uif_type5_ratio' % (start_day, end_day)] = actions_type_ratio['%s_%s_action_5' %
         (start_day, end_day)] / actions_type_ratio['%s_%s_uif_all_nums' % (start_day, end_day)]

        actions_type_ratio['%s_%s_uif_type6_ratio' % (start_day, end_day)] = actions_type_ratio['%s_%s_action_6' %
         (start_day, end_day)] / actions_type_ratio['%s_%s_uif_all_nums' % (start_day, end_day)]

        actions = actions_type_ratio[features]
        actions.to_csv(dump_path, index=False)
    return actions


# **计算用户当天对该商品操作的占比
def get_ui_basic_action_ratio(start_day, end_day):
    dump_path = '../../cache/uif_basic_action_ratio_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions_all_type = get_ui_basic_all_type_count(start_day,end_day)
        actions_all = get_actions(start_day,end_day)
        actions_ratio = actions_all[['user_id','sku_id']]
        actions_ratio = actions_ratio.groupby('user_id',as_index=False).count()
        actions_ratio.rename(columns=lambda x: x.replace('sku_id', 'total_nums'), inplace=True)
        actions_ratio = pd.merge(actions_all_type,actions_ratio,how='left',on=['user_id'])
        actions_ratio['%s_%s_uif_action_ration'% (start_day, end_day)] = \
            actions_ratio['%s_%s_uif_all_nums' % (start_day, end_day)]/actions_ratio['total_nums']
        actions = actions_ratio[['user_id','sku_id','%s_%s_uif_action_ration'% (start_day, end_day)]]
        actions.to_csv(dump_path, index=False)
    return actions


# -----------------------------------------
# XX计数特征 days 还有 hours
def get_ui_type_day_hour_count(end_day, days=range(1, 8), hours=[2, 6, 12]):
    hours.sort(reverse=True)
    days.sort()
    dump_path = '../../cache/uif_type_count_%s.csv' % end_day
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
            action_unit = get_ui_basic_count(start_day, end_day)
            end_day = start_day
            if actions_type_count is None:
                actions_type_count = action_unit
            else:
                actions_type_count = pd.merge(actions_type_count, action_unit, how='outer', on=['user_id', 'sku_id'])
        # 生成具体小时
        for i in hours:
            end_day = datetime_end_date.strftime('%Y-%m-%d')
            datetime_start_date = datetime_end_date - datetime.timedelta(hours=i)
            start_day = datetime_start_date.strftime('%Y-%m-%d %H:%M:%S')
            print start_day, end_day
            action_unit = get_ui_basic_count(start_day, end_day)
            actions_type_count = pd.merge(actions_type_count, action_unit, how='left', on=['user_id', 'sku_id'])
        actions = actions_type_count
        actions.to_csv(dump_path, index=False)
    return actions


# XX计数特征 acc ui_acc_days 的
def get_ui_type_accdays_count(end_day,accdays=[3, 5, 7, 10, 15]):
    accdays.sort(reverse=True)
    features=['user_id','sku_id']
    for i in accdays:
        features.append('%s_action1' %i)
        features.append('%s_action2' %i)
        features.append('%s_action3' %i)
        features.append('%s_action4' %i)
        features.append('%s_action5' %i)
        features.append('%s_action6' %i)
    glo_end = end_day
    dump_path = '../../cache/uif_type_acc_count_%s.csv' % end_day
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
            this = get_ui_basic_count(start_day, end_day)
            if day_count is None:
                day_count = this
            else:
                day_count = pd.merge(day_count, this, how='outer', on=['user_id', 'sku_id'])
        day_count = day_count.fillna(0)
        for this_peroid in accdays:
            dt_end_day = datetime.datetime.strptime(glo_end, '%Y-%m-%d')
            day_count['%s_action1' % this_peroid] = 0
            day_count['%s_action2' % this_peroid] = 0
            day_count['%s_action3' % this_peroid] = 0
            day_count['%s_action4' % this_peroid] = 0
            day_count['%s_action5' % this_peroid] = 0
            day_count['%s_action6' % this_peroid] = 0
            for this_day in range(1, this_peroid + 1):
                dt_start_day = dt_end_day - datetime.timedelta(days=1)
                start_day = dt_start_day.strftime('%Y-%m-%d')
                end_day = dt_end_day.strftime('%Y-%m-%d')
                print start_day, end_day, this_day,decay[this_day-1]
                dt_end_day = dt_start_day
                day_count['%s_action1' % this_peroid] += (day_count['%s_%s_action_1' % (start_day,end_day)] * decay[this_day-1])
                day_count['%s_action2' % this_peroid] += (day_count['%s_%s_action_2' % (start_day,end_day)] * decay[this_day-1])
                day_count['%s_action3' % this_peroid] += (day_count['%s_%s_action_3' % (start_day,end_day)] * decay[this_day-1])
                day_count['%s_action4' % this_peroid] += (day_count['%s_%s_action_4' % (start_day,end_day)] * decay[this_day-1])
                day_count['%s_action5' % this_peroid] += (day_count['%s_%s_action_5' % (start_day,end_day)] * decay[this_day-1])
                day_count['%s_action6' % this_peroid] += (day_count['%s_%s_action_6' % (start_day,end_day)] * decay[this_day-1])
        actions = day_count[features]
        actions.to_csv(dump_path, index=False)
    return actions


# XX比例特征 type占比特征
def get_ui_type_ratio_day_hour(end_day, days=range(1, 8), hours=[2, 6, 12]):
    hours.sort(reverse=True)
    days.sort()
    dump_path = '../../cache/uif_type_ratio_%s.csv' % end_day
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        datetime_end_date = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        actions_type_ratio = None
        # 生成具体天
        for i in days:
            datetime_start_date = datetime_end_date - datetime.timedelta(days=i)
            start_day = datetime_start_date.strftime('%Y-%m-%d')
            print start_day, end_day
            action_unit = get_ui_basic_type_ratio(start_day,end_day)
            end_day = start_day
            if actions_type_ratio is None:
                actions_type_ratio = action_unit
            else:
                actions_type_ratio = pd.merge(actions_type_ratio,action_unit,how='outer',on=['user_id','sku_id'])
        for i in hours:
            end_day = datetime_end_date.strftime('%Y-%m-%d')
            datetime_start_date = datetime_end_date - datetime.timedelta(hours=i)
            start_day = datetime_start_date.strftime('%Y-%m-%d %H:%M:%S')
            print start_day, end_day
            action_unit = get_ui_basic_type_ratio(start_day, end_day)
            actions_type_ratio = pd.merge(actions_type_ratio, action_unit, how='left', on=['user_id', 'sku_id'])
            actions = actions_type_ratio
        actions.to_csv(dump_path, index=False)
    return actions


# XX比例特征 显著性特征
def get_ui_action_ratio_day_hour(end_day, days=range(1, 8), hours=[2, 6, 12]):
    hours.sort(reverse=True)
    days.sort()
    dump_path = '../../cache/uif_action_ratio_day_hour_%s.csv' % end_day
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
            action_unit = get_ui_basic_action_ratio(start_day, end_day)
            end_day = start_day
            if actions_ratio is None:
                actions_ratio = action_unit
            else:
                actions_ratio = pd.merge(actions_ratio, action_unit, how='outer', on=['user_id', 'sku_id'])
        # 生成具体小时
        for i in hours:
            end_day = datetime_end_date.strftime('%Y-%m-%d')
            datetime_start_date = datetime_end_date - datetime.timedelta(hours=i)
            start_day = datetime_start_date.strftime('%Y-%m-%d %H:%M:%S')
            print start_day, end_day,i
            action_unit = get_ui_basic_action_ratio(start_day, end_day)
            actions_ratio = pd.merge(actions_ratio, action_unit, how='left', on=['user_id', 'sku_id'])
        actions = actions_ratio
        actions.to_csv(dump_path, index=False)
    return actions


# XX比例特征，显著性acc特征
def get_ui_action_accdays_ratio(end_day,accdays=[3, 5, 7, 10, 15]):
    accdays.sort(reverse=True)
    features = ['user_id', 'sku_id']
    for i in accdays:
        features.append('%s_action_ratio' % i)
    glo_end = end_day
    dump_path = '../../cache/uif_action_acc_ratio_%s.csv' % end_day
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
            this = get_ui_basic_all_type_count(start_day, end_day)
            if day_count is None:
                day_count = this
            else:
                day_count = pd.merge(day_count, this, how='outer', on=['user_id', 'sku_id'])
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
                    (day_count['%s_%s_uif_all_nums' % (start_day, end_day)] * decay[this_day - 1])
        left_columns = ['user_id','sku_id']
        for i in accdays:
            left_columns.append('%s_action_nums' % i)
        day_count = day_count[left_columns]
        # 计算用户操作的总数目
        for this_columns in accdays:
            day_all = day_count.groupby(['user_id'],as_index=False).agg({'%s_action_nums' % this_columns:'sum'})
            day_all.rename(columns=lambda x: x.replace('%s_action_nums' % this_columns, '%s_user_action_nums' % this_columns), inplace=True)
            day_count = pd.merge(day_count,day_all,how='left',on='user_id')
        for this_peroid in accdays:
            day_count['%s_action_ratio' % this_peroid] = day_count['%s_action_nums' % this_peroid] \
                                                         / day_count['%s_user_action_nums' % this_peroid]
        actions = day_count[features]
        actions.to_csv(dump_path, index=False)
    return actions


# --------------------------------------------
# $$ 计算用户在该商品上的活跃天数
def get_ui_active_days(start_day, end_day):
    dump_path = '../../cache/uif_active_days_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions = get_actions(start_day,end_day)
        active_days = actions.copy()[['user_id','sku_id','time']]
        active_days['time'] = active_days.loc[:, 'time'].apply(lambda x: x[0:10])
        active_days = active_days.drop_duplicates()
        active_days = active_days.groupby(['user_id','sku_id'],as_index=False).count()
        active_days.rename(columns=lambda x: x.replace('time', '%s_%s_uif_active_days' % (start_day, end_day)), inplace=True)
        actions = active_days
        actions.to_csv(dump_path, index=False)
    return actions


# $$ 之前写的代码太渣了。。这里找到的方法
def get_ui_time_features(start_day,end_day):
    dump_path = '../../cache/ui_time_features_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions = get_actions(start_day,end_day)
        ui_time = actions.copy()[['user_id','sku_id','time']]
        ui_time['time'] = ui_time.loc[:, 'time'].apply(lambda x: days_in_record[x[0:10]])
        ui_time = ui_time.drop_duplicates()
        ui_start_time = ui_time.groupby(['user_id','sku_id'],as_index=False).agg({'time':'min'})
        ui_start_time['uif_start_time'] = days_in_record[end_day] - ui_start_time['time']
        ui_end_time = ui_time.groupby(['user_id','sku_id'],as_index=False).agg({'time':'max'})
        ui_end_time['uif_end_time'] = days_in_record[end_day] - ui_end_time['time']
        del ui_start_time['time']
        del ui_end_time['time']
        ui_end_time = pd.merge(ui_start_time,ui_end_time,on=['user_id','sku_id'])
        actions = ui_end_time
        actions.to_csv(dump_path, index=False)
    return actions


# --------------------------------------------
# 排名特征
# 这个也需要按时间段去统计
# 这个只能计算当天的排名特征
def get_ui_basic_day_counter_rank(start_day,end_day):
    dump_path = '../../cache/ui_counter_rank_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions = get_actions(start_day,end_day)
        ui_all_rank = actions.copy()[['user_id','sku_id','type']]
        ui_all_rank = ui_all_rank.groupby(['user_id','sku_id'],as_index=False).count()
        ui_all_rank['uif_action_rank_%s_%s' % (start_day, end_day)] = ui_all_rank.groupby('user_id',as_index=False).rank(ascending=False,method='min')['type'].astype(int)
        del ui_all_rank['type']

        ui_type2_rank = actions.copy()[['user_id','sku_id','type']]
        ui_type2_rank = ui_type2_rank[ui_type2_rank['type']==2]
        if ui_type2_rank.empty:
            ui_type2_rank['uif_type2_rank_%s_%s' % (start_day, end_day)] = -1
        else:
            ui_type2_rank = ui_type2_rank.groupby(['user_id','sku_id'],as_index=False).count()
            ui_type2_rank['uif_type2_rank_%s_%s' % (start_day, end_day)] = ui_type2_rank.groupby('user_id',as_index=False).rank(ascending=False,method='min')['type'].astype(int)
        del ui_type2_rank['type']

        ui_type3_rank = actions.copy()[['user_id','sku_id','type']]
        ui_type3_rank = ui_type3_rank[ui_type3_rank['type']==3]
        if ui_type3_rank.empty:
            ui_type3_rank['uif_type3_rank_%s_%s' % (start_day, end_day)] = -1
        else:
            ui_type3_rank = ui_type3_rank.groupby(['user_id','sku_id'],as_index=False).count()
            ui_type3_rank['uif_type3_rank_%s_%s' % (start_day, end_day)] = ui_type3_rank.groupby('user_id',as_index=False).rank(ascending=False,method='min')['type'].astype(int)
        del ui_type3_rank['type']

        ui_type5_rank = actions.copy()[['user_id','sku_id','type']]
        ui_type5_rank = ui_type5_rank[ui_type5_rank['type']==5]
        if ui_type5_rank.empty:
            ui_type3_rank['uif_type3_rank_%s_%s' % (start_day, end_day)] = -1
        else:
            ui_type5_rank = ui_type5_rank.groupby(['user_id','sku_id'],as_index=False).count()
            ui_type5_rank['uif_type5_rank_%s_%s' % (start_day, end_day)] = ui_type5_rank.groupby('user_id',as_index=False).rank(ascending=False,method='min')['type'].astype(int)
        del ui_type5_rank['type']

        ui_all_rank = pd.merge(ui_all_rank,ui_type2_rank,how='left',on=['user_id','sku_id'])
        ui_all_rank = pd.merge(ui_all_rank,ui_type3_rank,how='left',on=['user_id','sku_id'])
        ui_all_rank = pd.merge(ui_all_rank,ui_type5_rank,how='left',on=['user_id','sku_id'])

        actions = ui_all_rank
        actions.to_csv(dump_path, index=False)
    return actions


def get_uic_basic_day_counter_rank(start_day,end_day):
    dump_path = '../../cache/uic_counter_rank_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions = get_actions(start_day,end_day)
        uic_all_rank = actions.copy()[['user_id','sku_id','type','cate']]
        uic_all_rank = uic_all_rank.groupby(['cate','user_id','sku_id'],as_index=False).count()
        uic_all_rank['uicf_action_rank_%s_%s' % (start_day, end_day)] = uic_all_rank.groupby('cate',as_index=False).rank(ascending=False,method='min')['type'].astype(int)
        del uic_all_rank['cate']
        del uic_all_rank['type']

        uic_type2_rank = actions.copy()[['user_id','sku_id','type','cate']]
        uic_type2_rank = uic_type2_rank[uic_type2_rank['type']==2]
        if uic_type2_rank.empty:
            uic_type2_rank['uicf_type2_rank_%s_%s' % (start_day, end_day)] = -1
        else:
            uic_type2_rank = uic_type2_rank.groupby(['cate','user_id','sku_id'],as_index=False).count()
            uic_type2_rank['uicf_type2_rank_%s_%s' % (start_day, end_day)] = uic_type2_rank.groupby('cate',as_index=False).rank(ascending=False,method='min')['type'].astype(int)
        del uic_type2_rank['cate']
        del uic_type2_rank['type']

        uic_all_rank = pd.merge(uic_all_rank,uic_type2_rank,how='left',on=['user_id','sku_id'])
        actions = uic_all_rank
        actions.to_csv(dump_path, index=False)
    return actions


def get_uib_basic_day_counter_rank(start_day,end_day):
    dump_path = '../../cache/uib_counter_rank_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions = get_actions(start_day,end_day)
        uib_all_rank = actions.copy()[['user_id','sku_id','type','brand']]
        uib_all_rank = uib_all_rank.groupby(['brand','user_id','sku_id'],as_index=False).count()
        uib_all_rank['uibf_action_rank_%s_%s' % (start_day, end_day)] = uib_all_rank.groupby('brand',as_index=False).rank(ascending=False,method='min')['type'].astype(int)
        del uib_all_rank['brand']
        del uib_all_rank['type']

        uib_type2_rank = actions.copy()[['user_id','sku_id','type','brand']]
        uib_type2_rank = uib_type2_rank[uib_type2_rank['type']==2]
        if uib_type2_rank.empty:
            uib_type2_rank['uibf_type2_rank_%s_%s' % (start_day, end_day)] = -1
        else:
            uib_type2_rank = uib_type2_rank.groupby(['brand','user_id','sku_id'],as_index=False).count()
            uib_type2_rank['uibf_type2_rank_%s_%s' % (start_day, end_day)] = uib_type2_rank.groupby('brand',as_index=False).rank(ascending=False,method='min')['type'].astype(int)
        del uib_type2_rank['brand']
        del uib_type2_rank['type']

        uib_all_rank = pd.merge(uib_all_rank,uib_type2_rank,how='left',on=['user_id','sku_id'])
        actions = uib_all_rank
        actions.to_csv(dump_path, index=False)
    return actions


def get_day_rank(end_day, days=range(1, 5)):
    dump_path = '../../cache/uif_day_rank_%s.csv' % end_day
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        day_rank = None
        datetime_end_date = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        # 生成具体天
        for i in days:
            datetime_start_date = datetime_end_date - datetime.timedelta(days=i)
            start_day = datetime_start_date.strftime('%Y-%m-%d')
            print start_day, end_day, i
            action_unit = get_ui_basic_day_counter_rank(start_day,end_day)
            uic_rank = get_uic_basic_day_counter_rank(start_day,end_day)
            uib_rank = get_uib_basic_day_counter_rank(start_day,end_day)
            action_unit = pd.merge(action_unit,uic_rank,how='outer',on=['user_id', 'sku_id'])
            action_unit = pd.merge(action_unit,uib_rank,how='outer',on=['user_id', 'sku_id'])
            end_day = start_day
            if day_rank is None:
                day_rank = action_unit
            else:
                day_rank = pd.merge(day_rank, action_unit, how='outer', on=['user_id', 'sku_id'])
        actions = day_rank
        actions.to_csv(dump_path, index=False)
    return actions


def get_ui_acc_counter_rank(end_day,accdays=[3, 5, 7, 10, 15]):
    features = ['user_id','sku_id']
    for i in accdays:
        features.append('uif_action_rank_%s' %i)
        features.append('uif_type2_rank_%s' %i)
        features.append('uif_type3_rank_%s' % i)
        features.append('uif_type5_rank_%s' % i)
    dump_path = '../../cache/ui_acc_counter_rank_%s.csv' % end_day
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        acc_rank = get_ui_type_accdays_count(end_day)
        for this_day in accdays:
            acc_rank['%s_actions' % this_day] = 0
            acc_rank['%s_actions' % this_day] += acc_rank['%s_action1' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_action2' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_action3' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_action4' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_action5' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_action6' % this_day]
        for this_day in accdays:
            acc_rank['uif_action_rank_%s' % this_day] = acc_rank.rank(ascending=False,method='min')['%s_actions' % this_day]
            acc_rank['uif_type2_rank_%s' % this_day] = acc_rank.rank(ascending=False,method='min')['%s_action2' % this_day]
            acc_rank['uif_type3_rank_%s' % this_day] = acc_rank.rank(ascending=False,method='min')['%s_action3' % this_day]
            acc_rank['uif_type5_rank_%s' % this_day] = acc_rank.rank(ascending=False,method='min')['%s_action5' % this_day]
        actions = acc_rank[features]
        actions.to_csv(dump_path, index=False)
    return actions


def get_uic_acc_counter_rank(end_day,accdays=[3, 5, 7, 10, 15]):
    features = ['user_id','sku_id']
    for i in accdays:
        features.append('uicf_action_rank_%s' %i)
        features.append('uicf_type2_rank_%s' %i)
    dump_path = '../../cache/uic_acc_counter_rank_%s.csv' % end_day
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        acc_rank = get_ui_type_accdays_count(end_day)
        for this_day in accdays:
            acc_rank['%s_actions' % this_day] = 0
            acc_rank['%s_actions' % this_day] += acc_rank['%s_action1' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_action2' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_action3' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_action4' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_action5' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_action6' % this_day]
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = dt_end_day - datetime.timedelta(days=15)
        start_day = dt_start_day.strftime('%Y-%m-%d')
        cate_info = get_actions(start_day,end_day)
        cate_info = cate_info[['user_id','sku_id','cate']]
        cate_info = cate_info.drop_duplicates()
        acc_rank = pd.merge(acc_rank,cate_info,how='left',on=['user_id','sku_id'])
        for this_day in accdays:
            acc_rank['uicf_action_rank_%s' % this_day] = \
                acc_rank.groupby('cate',as_index=False).rank(ascending=False,method='min')['%s_actions' % this_day].astype(int)
            acc_rank['uicf_type2_rank_%s' % this_day] = \
                acc_rank.groupby('cate',as_index=False).rank(ascending=False,method='min')['%s_action2' % this_day].astype(int)
        actions = acc_rank[features]
        actions.to_csv(dump_path, index=False)
    return actions


def get_uib_acc_counter_rank(end_day,accdays=[3, 5, 7, 10, 15]):
    features = ['user_id','sku_id']
    for i in accdays:
        features.append('uibf_action_rank_%s' %i)
        features.append('uibf_type2_rank_%s' %i)
    dump_path = '../../cache/uib_acc_counter_rank_%s.csv' % end_day
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        acc_rank = get_ui_type_accdays_count(end_day)
        for this_day in accdays:
            acc_rank['%s_actions' % this_day] = 0
            acc_rank['%s_actions' % this_day] += acc_rank['%s_action1' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_action2' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_action3' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_action4' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_action5' % this_day]
            acc_rank['%s_actions' % this_day] += acc_rank['%s_action6' % this_day]

        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = dt_end_day - datetime.timedelta(days=15)
        start_day = dt_start_day.strftime('%Y-%m-%d')

        brand_info = get_actions(start_day,end_day)
        brand_info = brand_info[['user_id','sku_id','brand']]
        brand_info = brand_info.drop_duplicates()
        acc_rank = pd.merge(acc_rank,brand_info,how='left',on=['user_id','sku_id'])
        for this_day in accdays:
            acc_rank['uibf_action_rank_%s' % this_day] = \
                acc_rank.groupby('brand',as_index=False).rank(ascending=False,method='min')['%s_actions' % this_day].astype(int)
            acc_rank['uibf_type2_rank_%s' % this_day] = \
                acc_rank.groupby('brand',as_index=False).rank(ascending=False,method='min')['%s_action2' % this_day].astype(int)
        actions = acc_rank[features]
        actions.to_csv(dump_path, index=False)
    return actions


# -------------------
def get_ui_features(start_day,end_day):
    dump_path = '../../cache/ui_all_features_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        # 计数特征
        # accdays
        print "开始计算UI特征"
        print "计数特征计算中~~~~"
        uif_counter_acc = get_ui_type_accdays_count(end_day)
        ui_features = uif_counter_acc
        # days & hours
        uif_counter_hour_day = get_ui_type_day_hour_count(end_day)
        ui_features = pd.merge(ui_features,uif_counter_hour_day,how='left',on=['user_id','sku_id'])

        # 比例特征
        print "比例特征计算中~~~"
        uif_type_ration = get_ui_type_ratio_day_hour(end_day)
        ui_features = pd.merge(ui_features,uif_type_ration,how='left',on=['user_id','sku_id'])

        # 操作显著性特征
        uif_action_ration = get_ui_action_ratio_day_hour(end_day)
        ui_features = pd.merge(ui_features,uif_action_ration,how='left',on=['user_id','sku_id'])
        uif_action_ration_acc = get_ui_action_accdays_ratio(end_day)
        ui_features = pd.merge(ui_features,uif_action_ration_acc,how='left',on=['user_id','sku_id'])

        # 对于前面的特征，存在0/0的可能，直接补零
        ui_features = ui_features.fillna(0)
        # 活跃度特征
        print "活跃度特征&时间特征~~~"
        uif_active_days = get_ui_active_days(start_day, end_day)
        ui_features = pd.merge(ui_features,uif_active_days,how='left',on=['user_id','sku_id'])
        uif_time_window = get_ui_time_features(start_day,end_day)
        ui_features = pd.merge(ui_features,uif_time_window,how='left',on=['user_id','sku_id'])

        # 排名特征
        print "排名特征~~~"
        print "每天的排名"
        uif_day_rank = get_day_rank(end_day)
        ui_features = pd.merge(ui_features,uif_day_rank,how='left',on=['user_id','sku_id'])
        print "累积排名"
        uif_ui_rank = get_ui_acc_counter_rank(end_day)
        uif_uic_rank = get_uic_acc_counter_rank(end_day)
        uif_uib_rank = get_uib_acc_counter_rank(end_day)
        ui_features = pd.merge(ui_features,uif_ui_rank,how='left',on=['user_id','sku_id'])
        ui_features = pd.merge(ui_features,uif_uic_rank,how='left',on=['user_id','sku_id'])
        ui_features = pd.merge(ui_features,uif_uib_rank,how='left',on=['user_id','sku_id'])

        # -1表示未知
        ui_features = ui_features.fillna(-1)
        print "UI特征完结 撒花！！！"
        actions = ui_features
        actions.to_csv(dump_path, index=False)
    return actions


if __name__ == '__main__':
    start_date = '2016-02-01'
    end_date = '2016-03-17'
    start = time.clock()
    # get_ui_features('2016-03-11','2016-04-11')
    get_ui_features('2016-02-01', '2016-03-17')
    end = time.clock()
    print("The function run time is : %.03f seconds" % (end - start))
