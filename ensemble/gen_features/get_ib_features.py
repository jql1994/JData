# coding=utf-8
import pandas as pd
import os
import datetime
import time
import numpy as np
from data_path import *
from get_actions import get_actions
from get_brand_features import get_brand_count_user_features
from get_item_features import get_item_decayed_count_features


# 非衰减排名
def get_ib_rank_features(start_day, end_day):
    dump_path = '../../cache/ib_rank_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        actions = get_actions(start_day,end_day)
        ib_all_rank = actions.copy()[['brand','sku_id','type']]
        ib_all_rank = ib_all_rank.groupby(['brand','sku_id'],as_index=False).count()
        ib_all_rank['%s_ibf_all_rank' % key] = ib_all_rank.groupby('brand',as_index=False).rank(ascending=False,method='min')['type'].astype(int)
        del ib_all_rank['type']

        ib_type2_rank = actions.copy()[['sku_id','brand','type']]
        ib_type2_rank = ib_type2_rank[ib_type2_rank['type']==2]
        ib_type2_rank = ib_type2_rank.groupby(['brand','sku_id'],as_index=False).count()
        ib_type2_rank['%s_ibf_type2_rank' % key] = ib_type2_rank.groupby('brand',as_index=False).rank(ascending=False,method='min')['type'].astype(int)
        del ib_type2_rank['type']

        ib_type3_rank = actions.copy()[['sku_id','brand','type']]
        ib_type3_rank = ib_type3_rank[ib_type3_rank['type']==3]
        ib_type3_rank = ib_type3_rank.groupby(['brand','sku_id'],as_index=False).count()
        ib_type3_rank['%s_ibf_type3_rank' % key] = ib_type3_rank.groupby('brand',as_index=False).rank(ascending=False,method='min')['type'].astype(int)
        del ib_type3_rank['type']

        ib_type4_rank = actions.copy()[['sku_id','brand','type']]
        ib_type4_rank = ib_type4_rank[ib_type4_rank['type']==4]
        ib_type4_rank = ib_type4_rank.groupby(['brand','sku_id'],as_index=False).count()
        ib_type4_rank['%s_ibf_type4_rank' % key] = ib_type4_rank.groupby('brand',as_index=False).rank(ascending=False,method='min')['type'].astype(int)
        del ib_type4_rank['type']

        ib_type5_rank = actions.copy()[['sku_id','brand','type']]
        ib_type5_rank = ib_type5_rank[ib_type5_rank['type']==5]
        ib_type5_rank = ib_type5_rank.groupby(['brand','sku_id'],as_index=False).count()
        ib_type5_rank['%s_ibf_type5_rank' % key] = ib_type5_rank.groupby('brand',as_index=False).rank(ascending=False,method='min')['type'].astype(int)
        del ib_type5_rank['type']

        ib_all_rank = pd.merge(ib_all_rank, ib_type2_rank, how='left', on=['brand','sku_id'])
        ib_all_rank = pd.merge(ib_all_rank, ib_type3_rank, how='left', on=['brand','sku_id'])
        ib_all_rank = pd.merge(ib_all_rank, ib_type4_rank, how='left', on=['brand', 'sku_id'])
        ib_all_rank = pd.merge(ib_all_rank, ib_type5_rank, how='left', on=['brand', 'sku_id'])
        actions = ib_all_rank
        actions.to_csv(dump_path, index=False)
    return actions


# 衰减排名
def get_ib_decayed_rank_features(start_day, end_day):
    dump_path = '../../cache/ib_decayed_rank_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        ib_all_rank = get_item_decayed_count_features(start_day,end_day)

        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        features = ['sku_id','brand','%s_ibf_decayed_all_rank' % key, '%s_ibf_decayed_type1_rank' % key,
                    '%s_ibf_decayed_type2_rank' % key,'%s_ibf_decayed_type3_rank' % key,
                    '%s_ibf_decayed_type4_rank' % key,'%s_ibf_decayed_type5_rank' % key,
                    '%s_ibf_decayed_type6_rank' % key]

        ib_all_rank['actions'] = 0
        ib_all_rank['actions'] += ib_all_rank['%s_if_type1_dnums' % key]
        ib_all_rank['actions'] += ib_all_rank['%s_if_type2_dnums' % key]
        ib_all_rank['actions'] += ib_all_rank['%s_if_type3_dnums' % key]
        ib_all_rank['actions'] += ib_all_rank['%s_if_type4_dnums' % key]
        ib_all_rank['actions'] += ib_all_rank['%s_if_type5_dnums' % key]
        ib_all_rank['actions'] += ib_all_rank['%s_if_type6_dnums' % key]

        brand_info = get_actions(start_day,end_day)
        brand_info = brand_info[['sku_id','brand']]
        brand_info = brand_info.drop_duplicates()
        ib_all_rank = pd.merge(ib_all_rank,brand_info,how='left',on='sku_id')

        ib_all_rank['%s_ibf_decayed_all_rank' % key] = ib_all_rank.groupby('brand',as_index=False).rank(ascending=False,method='min')['actions']
        ib_all_rank['%s_ibf_decayed_type1_rank' % key] = ib_all_rank.groupby('brand',as_index=False).rank(ascending=False,method='min')['%s_if_type1_dnums' % key]
        ib_all_rank['%s_ibf_decayed_type2_rank' % key] = ib_all_rank.groupby('brand',as_index=False).rank(ascending=False,method='min')['%s_if_type2_dnums' % key]
        ib_all_rank['%s_ibf_decayed_type3_rank' % key] = ib_all_rank.groupby('brand',as_index=False).rank(ascending=False,method='min')['%s_if_type3_dnums' % key]
        ib_all_rank['%s_ibf_decayed_type4_rank' % key] = ib_all_rank.groupby('brand',as_index=False).rank(ascending=False,method='min')['%s_if_type4_dnums' % key]
        ib_all_rank['%s_ibf_decayed_type5_rank' % key] = ib_all_rank.groupby('brand',as_index=False).rank(ascending=False,method='min')['%s_if_type5_dnums' % key]
        ib_all_rank['%s_ibf_decayed_type6_rank' % key] = ib_all_rank.groupby('brand',as_index=False).rank(ascending=False,method='min')['%s_if_type6_dnums' % key]

        actions = ib_all_rank[features]

        actions.to_csv(dump_path, index=False)
    return actions


# 在这个brand里该商品占比
# type2,type4,all
def get_ib_ratio_features(start_day, end_day):
    feature = ['brand','sku_id','ibf_type1_ratio','ibf_type2_ratio','ibf_type3_ratio',
               'ibf_type4_ratio','ibf_type5_ratio','ibf_type6_ratio']
    dump_path = '../../cache/ib_ratio_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        brand_user = get_brand_count_user_features(start_day,end_day)
        actions = get_actions(start_day,end_day)
        ib_ratio = actions[['user_id','sku_id','brand','type']]
        ib_ratio = ib_ratio.drop_duplicates()
        df = pd.get_dummies(ib_ratio['type'], prefix='action')
        ib_ratio = pd.concat([ib_ratio, df], axis=1)
        del ib_ratio['user_id']
        del ib_ratio['type']
        ib_ratio = ib_ratio.groupby(['brand','sku_id'], as_index=False).sum()
        ib_ratio = pd.merge(ib_ratio,brand_user,how='left',on=['brand'])
        ib_ratio['ibf_type1_ratio'] = ib_ratio['action_1']/ib_ratio['%s_bf_type1_user' % key]
        ib_ratio['ibf_type2_ratio'] = ib_ratio['action_2']/ib_ratio['%s_bf_type2_user' % key]
        ib_ratio['ibf_type3_ratio'] = ib_ratio['action_3']/ib_ratio['%s_bf_type3_user' % key]
        ib_ratio['ibf_type4_ratio'] = ib_ratio['action_4']/ib_ratio['%s_bf_type4_user' % key]
        ib_ratio['ibf_type5_ratio'] = ib_ratio['action_5']/ib_ratio['%s_bf_type5_user' % key]
        ib_ratio['ibf_type6_ratio'] = ib_ratio['action_6']/ib_ratio['%s_bf_type6_user' % key]
        actions = ib_ratio[feature]
        actions.to_csv(dump_path, index=False)
    return actions


# 默认计算全局及7天&15天内热度
def get_ib_features(start_day, end_day):
    dump_path = '../../cache/ib_all_features_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        print "开始计算IB特征"
        print "Rank"
        # 生成时间间隔 15,7，3
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = dt_end_day - datetime.timedelta(days=15)
        start_15day = dt_start_day.strftime('%Y-%m-%d')
        dt_start_day = dt_end_day - datetime.timedelta(days=7)
        start_7day = dt_start_day.strftime('%Y-%m-%d')
        dt_start_day = dt_end_day - datetime.timedelta(days=3)
        start_3day = dt_start_day.strftime('%Y-%m-%d')

        ib_features = get_ib_decayed_rank_features(start_day,end_day)
        ib_rank = get_ib_rank_features(start_day,end_day)
        ib_features = pd.merge(ib_features,ib_rank,how='left',on=['brand','sku_id'])

        print "3,7,15"
        ib_rank = get_ib_rank_features(start_3day,end_day)
        ib_features = pd.merge(ib_features,ib_rank,how='left',on=['brand','sku_id'])
        ib_rank = get_ib_rank_features(start_7day, end_day)
        ib_features = pd.merge(ib_features,ib_rank,how='left',on=['brand','sku_id'])
        ib_rank = get_ib_rank_features(start_15day, end_day)
        ib_features = pd.merge(ib_features,ib_rank,how='left',on=['brand','sku_id'])

        print "ratio"
        ib_ratio = get_ib_ratio_features(start_day,end_day)
        ib_features = pd.merge(ib_features,ib_ratio,how='left',on=['brand','sku_id'])
        actions = ib_features
        print "IB特征完结 撒花！！"
        actions.to_csv(dump_path, index=False)
    return actions


# if __name__ == '__main__':
#     start_date = '2016-02-08'
#     end_date = '2016-03-08'
#     # this = get_ib_rank_features(start_date,end_date)
#     # this = get_ib_ratio_features(start_date,end_date)
#     this = get_ib_features(start_date,end_date)
#     # this = get_ib_decayed_rank_features(start_date,end_date)
#     print this.columns
#     # get_ib_rank_features()
#     # get_ib_ratio_features()
#     # get_ib_ratio_features('2016-03-11', '2016-04-11')