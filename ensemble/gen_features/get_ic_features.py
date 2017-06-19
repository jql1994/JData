# coding=utf-8
import pandas as pd
import os
import datetime
import time
import numpy as np
from data_path import *
from get_actions import get_actions
from get_cate_features import get_cate_count_user_features
from get_item_features import get_item_decayed_count_features


# 非衰减排名
def get_ic_rank_features(start_day, end_day):
    dump_path = '../../cache/ic_rank_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        actions = get_actions(start_day,end_day)
        ic_all_rank = actions.copy()[['cate','sku_id','type']]
        ic_all_rank = ic_all_rank.groupby(['cate','sku_id'],as_index=False).count()
        ic_all_rank['%s_icf_all_rank' % key] = ic_all_rank.groupby('cate',as_index=False).rank(ascending=False,method='min')['type'].astype(int)
        del ic_all_rank['type']

        ic_type2_rank = actions.copy()[['sku_id','cate','type']]
        ic_type2_rank = ic_type2_rank[ic_type2_rank['type']==2]
        ic_type2_rank = ic_type2_rank.groupby(['cate','sku_id'],as_index=False).count()
        ic_type2_rank['%s_icf_type2_rank' % key] = ic_type2_rank.groupby('cate',as_index=False).rank(ascending=False,method='min')['type'].astype(int)
        del ic_type2_rank['type']

        ic_type3_rank = actions.copy()[['sku_id','cate','type']]
        ic_type3_rank = ic_type3_rank[ic_type3_rank['type']==3]
        ic_type3_rank = ic_type3_rank.groupby(['cate','sku_id'],as_index=False).count()
        ic_type3_rank['%s_icf_type3_rank' % key] = ic_type3_rank.groupby('cate',as_index=False).rank(ascending=False,method='min')['type'].astype(int)
        del ic_type3_rank['type']

        ic_type4_rank = actions.copy()[['sku_id','cate','type']]
        ic_type4_rank = ic_type4_rank[ic_type4_rank['type']==4]
        ic_type4_rank = ic_type4_rank.groupby(['cate','sku_id'],as_index=False).count()
        ic_type4_rank['%s_icf_type4_rank' % key] = ic_type4_rank.groupby('cate',as_index=False).rank(ascending=False,method='min')['type'].astype(int)
        del ic_type4_rank['type']

        ic_type5_rank = actions.copy()[['sku_id','cate','type']]
        ic_type5_rank = ic_type5_rank[ic_type5_rank['type']==5]
        ic_type5_rank = ic_type5_rank.groupby(['cate','sku_id'],as_index=False).count()
        ic_type5_rank['%s_icf_type5_rank' % key] = ic_type5_rank.groupby('cate',as_index=False).rank(ascending=False,method='min')['type'].astype(int)
        del ic_type5_rank['type']

        ic_all_rank = pd.merge(ic_all_rank, ic_type2_rank, how='left', on=['cate','sku_id'])
        ic_all_rank = pd.merge(ic_all_rank, ic_type3_rank, how='left', on=['cate','sku_id'])
        ic_all_rank = pd.merge(ic_all_rank, ic_type4_rank, how='left', on=['cate', 'sku_id'])
        ic_all_rank = pd.merge(ic_all_rank, ic_type5_rank, how='left', on=['cate', 'sku_id'])
        actions = ic_all_rank
        actions.to_csv(dump_path, index=False)
    return actions


# 衰减排名
def get_ic_decayed_rank_features(start_day, end_day):
    dump_path = '../../cache/ic_decayed_rank_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        ic_all_rank = get_item_decayed_count_features(start_day,end_day)

        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        features = ['sku_id','cate','%s_icf_decayed_all_rank' % key, '%s_icf_decayed_type1_rank' % key,
                    '%s_icf_decayed_type2_rank' % key,'%s_icf_decayed_type3_rank' % key,
                    '%s_icf_decayed_type4_rank' % key,'%s_icf_decayed_type5_rank' % key,
                    '%s_icf_decayed_type6_rank' % key]

        ic_all_rank['actions'] = 0
        ic_all_rank['actions'] += ic_all_rank['%s_if_type1_dnums' % key]
        ic_all_rank['actions'] += ic_all_rank['%s_if_type2_dnums' % key]
        ic_all_rank['actions'] += ic_all_rank['%s_if_type3_dnums' % key]
        ic_all_rank['actions'] += ic_all_rank['%s_if_type4_dnums' % key]
        ic_all_rank['actions'] += ic_all_rank['%s_if_type5_dnums' % key]
        ic_all_rank['actions'] += ic_all_rank['%s_if_type6_dnums' % key]

        cate_info = get_actions(start_day,end_day)
        cate_info = cate_info[['sku_id','cate']]
        cate_info = cate_info.drop_duplicates()
        ic_all_rank = pd.merge(ic_all_rank,cate_info,how='left',on='sku_id')

        ic_all_rank['%s_icf_decayed_all_rank' % key] = ic_all_rank.groupby('cate',as_index=False).rank(ascending=False,method='min')['actions']
        ic_all_rank['%s_icf_decayed_type1_rank' % key] = ic_all_rank.groupby('cate',as_index=False).rank(ascending=False,method='min')['%s_if_type1_dnums' % key]
        ic_all_rank['%s_icf_decayed_type2_rank' % key] = ic_all_rank.groupby('cate',as_index=False).rank(ascending=False,method='min')['%s_if_type2_dnums' % key]
        ic_all_rank['%s_icf_decayed_type3_rank' % key] = ic_all_rank.groupby('cate',as_index=False).rank(ascending=False,method='min')['%s_if_type3_dnums' % key]
        ic_all_rank['%s_icf_decayed_type4_rank' % key] = ic_all_rank.groupby('cate',as_index=False).rank(ascending=False,method='min')['%s_if_type4_dnums' % key]
        ic_all_rank['%s_icf_decayed_type5_rank' % key] = ic_all_rank.groupby('cate',as_index=False).rank(ascending=False,method='min')['%s_if_type5_dnums' % key]
        ic_all_rank['%s_icf_decayed_type6_rank' % key] = ic_all_rank.groupby('cate',as_index=False).rank(ascending=False,method='min')['%s_if_type6_dnums' % key]

        actions = ic_all_rank[features]

        actions.to_csv(dump_path, index=False)
    return actions


# 在这个cate里该商品占比
# type2,type4,all
def get_ic_ratio_features(start_day, end_day):
    feature = ['cate','sku_id','icf_type1_ratio','icf_type2_ratio','icf_type3_ratio',
               'icf_type4_ratio','icf_type5_ratio','icf_type6_ratio']
    dump_path = '../../cache/ic_ratio_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = datetime.datetime.strptime(start_day, '%Y-%m-%d')
        key = (dt_end_day - dt_start_day).days

        cate_user = get_cate_count_user_features(start_day,end_day)
        actions = get_actions(start_day,end_day)
        ic_ratio = actions[['user_id','sku_id','cate','type']]
        ic_ratio = ic_ratio.drop_duplicates()
        df = pd.get_dummies(ic_ratio['type'], prefix='action')
        ic_ratio = pd.concat([ic_ratio, df], axis=1)
        del ic_ratio['user_id']
        del ic_ratio['type']
        ic_ratio = ic_ratio.groupby(['cate','sku_id'], as_index=False).sum()
        ic_ratio = pd.merge(ic_ratio,cate_user,how='left',on=['cate'])
        ic_ratio['icf_type1_ratio'] = ic_ratio['action_1']/ic_ratio['%s_cf_type1_user' % key]
        ic_ratio['icf_type2_ratio'] = ic_ratio['action_2']/ic_ratio['%s_cf_type2_user' % key]
        ic_ratio['icf_type3_ratio'] = ic_ratio['action_3']/ic_ratio['%s_cf_type3_user' % key]
        ic_ratio['icf_type4_ratio'] = ic_ratio['action_4']/ic_ratio['%s_cf_type4_user' % key]
        ic_ratio['icf_type5_ratio'] = ic_ratio['action_5']/ic_ratio['%s_cf_type5_user' % key]
        ic_ratio['icf_type6_ratio'] = ic_ratio['action_6']/ic_ratio['%s_cf_type6_user' % key]
        actions = ic_ratio[feature]
        actions.to_csv(dump_path, index=False)
    return actions


# 默认计算全局及7天&15天内热度
def get_ic_features(start_day, end_day):
    dump_path = '../../cache/ic_all_features_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        print "开始计算ic特征"
        print "Rank"
        # 生成时间间隔 15,7，3
        dt_end_day = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        dt_start_day = dt_end_day - datetime.timedelta(days=15)
        start_15day = dt_start_day.strftime('%Y-%m-%d')
        dt_start_day = dt_end_day - datetime.timedelta(days=7)
        start_7day = dt_start_day.strftime('%Y-%m-%d')
        dt_start_day = dt_end_day - datetime.timedelta(days=3)
        start_3day = dt_start_day.strftime('%Y-%m-%d')

        ic_features = get_ic_decayed_rank_features(start_day,end_day)
        ic_rank = get_ic_rank_features(start_day,end_day)
        ic_features = pd.merge(ic_features,ic_rank,how='left',on=['cate','sku_id'])

        print "3,7,15"
        ic_rank = get_ic_rank_features(start_3day,end_day)
        ic_features = pd.merge(ic_features,ic_rank,how='left',on=['cate','sku_id'])
        ic_rank = get_ic_rank_features(start_7day, end_day)
        ic_features = pd.merge(ic_features,ic_rank,how='left',on=['cate','sku_id'])
        ic_rank = get_ic_rank_features(start_15day, end_day)
        ic_features = pd.merge(ic_features,ic_rank,how='left',on=['cate','sku_id'])

        print "ratio"
        ic_ratio = get_ic_ratio_features(start_day,end_day)
        ic_features = pd.merge(ic_features,ic_ratio,how='left',on=['cate','sku_id'])
        actions = ic_features
        print "ic特征完结 撒花！！"
        actions.to_csv(dump_path, index=False)
    return actions


# if __name__ == '__main__':
#     start_date = '2016-02-08'
#     end_date = '2016-03-08'
#     # this = get_ic_rank_features(start_date,end_date)
#     # this = get_ic_ratio_features(start_date,end_date)
#     this = get_ic_features(start_date,end_date)
#     # this = get_ic_decayed_rank_features(start_date,end_date)
#     print this.columns
#     # get_ic_rank_features()
#     # get_ic_ratio_features()
#     # get_ic_ratio_features('2016-03-11', '2016-04-11')