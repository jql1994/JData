# coding=utf-8
import pandas as pd
import os
import datetime
import time
from gen_features.get_actions import get_actions
from gen_features.get_user_features import get_user_features
from gen_features.get_item_features import get_item_features
from gen_features.get_brand_features import get_brand_features
from gen_features.get_cate_features import get_cate_features
from gen_features.get_ib_features import get_ib_features
from gen_features.get_ic_features import get_ic_features
from gen_features.get_ui_features import get_ui_features
from gen_features.get_ub_features import get_ub_features
from gen_features.get_uc_features import get_uc_features
from gen_features.get_comment_features import get_comment_features
from get_labels import get_labels


# per周期，interval时间间隔
# 返回值 rounds（int）表示
# 3.15日当天操作量巨大。


def get_train_rounds(per, interval):

    """
    :param per:
    :param interval:
    :return: rounds: int T_dic 轮次

    """

    T_diction = []
    start_date = datetime.date(2016, 2, 1)
    end_date = datetime.date(2016, 4, 15)
    train_start_date = start_date
    train_end_date = start_date + datetime.timedelta(days=per)
    test_start_date = train_end_date
    test_end_date = test_start_date + datetime.timedelta(days=interval)
    counter = 1
    while test_end_date < end_date:
        formated_train_start_date = train_start_date.strftime('%Y-%m-%d')
        formated_train_end_date = train_end_date.strftime('%Y-%m-%d')
        formated_test_start_date = test_start_date.strftime('%Y-%m-%d')
        formated_test_end_date = test_end_date.strftime('%Y-%m-%d')
        print "--------------Round %s -----------" % counter
        print 'train_start_date : ', formated_train_start_date
        print 'train_end_date : ', formated_train_end_date
        print 'test_start_date : ', formated_test_start_date
        print 'test_end_date : ', formated_test_end_date
        this = [formated_train_start_date, formated_train_end_date, formated_test_start_date, formated_test_end_date]
        T_diction.append(this)
        counter += 1
        train_start_date = train_start_date + datetime.timedelta(days=interval)
        train_end_date = train_start_date + datetime.timedelta(days=per)
        test_start_date = train_end_date
        test_end_date = test_start_date + datetime.timedelta(days=5)
    rounds = counter - 1
    return rounds, T_diction


def get_train_set(start_day,end_day):
    dump_path = '../../cache/train_set_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        # infos = get_actions(start_day,end_day)
        # infos = infos[['sku_id','cate','brand']]
        # infos = infos.drop_duplicates()
        uf_features = get_user_features(start_day,end_day)
        if_features = get_item_features(start_day,end_day)
        bf_features = get_brand_features(start_day,end_day)
        cf_features = get_cate_features(start_day,end_day)
        ib_features = get_ib_features(start_day,end_day)
        ic_features = get_ic_features(start_day,end_day)
        comment_features = get_comment_features(end_day)
        ub_features = get_ub_features(start_day,end_day)
        uc_features = get_uc_features(start_day,end_day)
        actions = get_ui_features(start_day,end_day)
        # actions = pd.merge(actions,infos,how='left',on=['sku_id'])
        actions = pd.merge(actions,uf_features,how='left',on=['user_id'])
        actions = pd.merge(actions,if_features,how='left',on=['sku_id'])
        actions = pd.merge(actions,bf_features,how='left',on=['brand'])
        actions = pd.merge(actions,cf_features,how='left',on=['cate'])
        actions = pd.merge(actions,ub_features,how='left',on=['user_id','brand'])
        actions = pd.merge(actions,uc_features,how='left',on=['user_id','cate'])
        actions = pd.merge(actions,ib_features,how='left',on=['sku_id','brand'])
        actions = pd.merge(actions,ic_features,how='left',on=['sku_id','cate'])
        actions = pd.merge(actions,comment_features,how='left',on=['sku_id'])
        actions = actions.fillna(-1)
        actions.to_csv(dump_path, index=False)
    return actions


def get_test_set(start_day,end_day):
    dump_path = '../../cache/test_set_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        uf_features = get_user_features(start_day,end_day)
        if_features = get_item_features(start_day,end_day)
        bf_features = get_brand_features(start_day,end_day)
        cf_features = get_cate_features(start_day,end_day)
        ib_features = get_ib_features(start_day,end_day)
        ic_features = get_ic_features(start_day,end_day)
        comment_features = get_comment_features(end_day)
        ub_features = get_ub_features(start_day,end_day)
        uc_features = get_uc_features(start_day,end_day)
        actions = get_ui_features(start_day,end_day)
        actions = pd.merge(actions,uf_features,how='left',on=['user_id'])
        actions = pd.merge(actions,if_features,how='left',on=['sku_id'])
        actions = pd.merge(actions,bf_features,how='left',on=['brand'])
        actions = pd.merge(actions,cf_features,how='left',on=['cate'])
        actions = pd.merge(actions,ub_features,how='left',on=['user_id','brand'])
        actions = pd.merge(actions,uc_features,how='left',on=['user_id','cate'])
        actions = pd.merge(actions,ib_features,how='left',on=['sku_id','brand'])
        actions = pd.merge(actions,ic_features,how='left',on=['sku_id','cate'])
        actions = pd.merge(actions,comment_features,how='left',on=['sku_id'])
        actions = actions.fillna(-1)

        actions = actions[actions['cate'] == 8]

        datetype_test_end_date = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        labels_end_date = (datetype_test_end_date + datetime.timedelta(days=5)).strftime('%Y-%m-%d')
        labels = get_labels(end_day, labels_end_date)

        # 删掉非连续特征
        #         del actions['cate']
        del actions['brand']

        actions = pd.merge(actions, labels, how='left', on=['user_id', 'sku_id'])
        actions = actions.fillna(0)

        actions.to_csv(dump_path, index=False)
    return actions


def processed_train_set(start_day,end_day):
    dump_path = '../../cache/processed_train_set_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions = get_train_set(start_day,end_day)

        datetype_test_end_date = datetime.datetime.strptime(end_day, '%Y-%m-%d')
        labels_end_date = (datetype_test_end_date + datetime.timedelta(days=5)).strftime('%Y-%m-%d')
        labels = get_labels(end_day, labels_end_date)

# 删掉非连续特征
#         del actions['cate']
        del actions['brand']

        actions = pd.merge(actions, labels, how='left', on=['user_id', 'sku_id'])
        actions = actions.fillna(0)
        # actions = actions[actions['cate'] == 8]

        actions.to_csv(dump_path, index=False)
    ui_pair = actions[['user_id', 'sku_id']].copy()
    labels = actions['label'].copy()
    # del actions['user_id']
    # del actions['sku_id']
    del actions['label']
    return ui_pair,actions,labels


# --------------------------------
if __name__ == '__main__':
    # # print get_decay(range(1,10))
    nums,rounds = get_train_rounds(45,3)
    # counter = 0
    # for this_round in rounds:
    #     if 1 <= counter < 2:
    #         print this_round[0],this_round[1]
    #         # processed_train_set(this_round[0],this_round[1])
    #     counter +=1
    get_test_set('2016-02-19','2016-04-04')