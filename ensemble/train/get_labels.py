# coding=utf-8
import pandas as pd
import os
from gen_features.get_actions import get_actions
from gen_features.get_item_features import get_basic_item_features


def get_labels(start_day, end_day):
    dump_path = '../../cache/labels_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions = get_actions(start_day, end_day)
        actions = actions[actions['type'] == 4]
        actions = actions.groupby(['user_id', 'sku_id'], as_index=False).sum()
        actions['label'] = 1
        actions = actions[['user_id', 'sku_id', 'label']]
        actions.to_csv(dump_path, index=False)
    return actions


def get_evaluation_labels(start_day, end_day):
    dump_path = '../../cache/eva_labels_%s_%s.csv' % (start_day, end_day)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        actions = get_actions(start_day, end_day)
        actions = actions[actions['type'] == 4]
        actions = actions.groupby(['user_id', 'sku_id'], as_index=False).sum()
        actions['label'] = 1
        actions = actions[['user_id', 'sku_id', 'label']]

        product = get_basic_item_features()
        product = product[['sku_id','cate']]
        actions = pd.merge(actions,product,how='left',on='sku_id')
        actions = actions[actions['cate'] == 8]

        actions = actions[['user_id', 'sku_id', 'label']]
        actions.to_csv(dump_path, index=False)
    return actions

if __name__ == '__main__':
    start_date = '2016-04-04'
    end_date = '2016-04-09'
    get_evaluation_labels(start_date,end_date)

