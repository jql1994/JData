# coding=utf-8
import pandas as pd
import os
from get_actions import get_all_actions, get_actions


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


if __name__ == '__main__':
    start_date = '2016-04-11'
    end_date = '2016-04-16'
    get_labels(start_date,end_date)

