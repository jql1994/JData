# coding=utf-8
import os
import pandas as pd
from data_path import *
import datetime


def get_month2_action():
    action = pd.read_csv(action_month2_path)
    return action


def get_month3_action():
    action = pd.read_csv(action_month3_path)
    return action


def get_month4_action():
    action = pd.read_csv(action_month4_path)
    return action


# 把所有的action联合在一起
def get_all_actions():
    dump_path = "../../cache/all_actions.csv"
    if os.path.exists(dump_path):
        return pd.read_csv(dump_path)
    else:
        action_in_month2 = get_month2_action()
        action_in_month3 = get_month3_action()
        action_in_month4 = get_month4_action()
        actions = pd.concat([action_in_month2, action_in_month3, action_in_month4])
        actions.to_csv(dump_path, index=False)
        return actions


# 获得指定日期内的action
def get_actions(start_date, end_date):
    """
    :param start_date:
    :param end_date:
    :return: actions: pd.Dataframe
    """
    dump_path = "../../cache/actions_in_%s_%s.csv" % (start_date, end_date)
    days = days_in_record[end_date[0:10]]-days_in_record[start_date[0:10]]
    if os.path.exists(dump_path):
        return pd.read_csv(dump_path)
    # elif days <=1 :
    #     get_simple_actions(start_date, end_date)
    else:
        actions = get_all_actions()
        actions = actions[(actions.time >= start_date) & (actions.time < end_date)]
        actions.to_csv(dump_path, index=False)
        return actions


def split_actions():
    dump_path1 = "../../cache/actions_part1.csv"
    dump_path2 = "../../cache/actions_part2.csv"
    dump_path3 = "../../cache/actions_part3.csv"
    dump_path4 = "../../cache/actions_part4.csv"
    dump_path5 = "../../cache/actions_part5.csv"
    dump_path6 = "../../cache/actions_part6.csv"
    dump_path7 = "../../cache/actions_part7.csv"
    actions = get_all_actions()
    part1 = '2016-02-15'
    part2 = '2016-02-29'
    part3 = '2016-03-10'
    part4 = '2016-03-20'
    part5 = '2016-03-30'
    part6 = '2016-04-07'
    actions_part1 = actions[(actions.time < part1)]
    actions_part2 = actions[(actions.time >= part1) & (actions.time < part2)]
    actions_part3 = actions[(actions.time >= part2) & (actions.time < part3)]
    actions_part4 = actions[(actions.time >= part3) & (actions.time < part4)]
    actions_part5 = actions[(actions.time >= part4) & (actions.time < part5)]
    actions_part6 = actions[(actions.time >= part5) & (actions.time < part6)]
    actions_part7 = actions[(actions.time >= part6)]
    actions_part1.to_csv(dump_path1, index=False)
    actions_part2.to_csv(dump_path2, index=False)
    actions_part3.to_csv(dump_path3, index=False)
    actions_part4.to_csv(dump_path4, index=False)
    actions_part5.to_csv(dump_path5, index=False)
    actions_part6.to_csv(dump_path6, index=False)
    actions_part7.to_csv(dump_path7, index=False)


def get_simple_actions(start_date, end_date):
    start_day = start_date[0:10]
    end_day = end_date[0:10]
    days = days_in_record[end_date[0:10]]-days_in_record[start_date[0:10]]
    if days == 1:
        return pd.read_csv("../../cache/actions_in_%s_%s.csv" % (start_date, end_date))
    else:
        dt_start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        dt_end_date = dt_start_date + datetime.timedelta(days=1)
        end_date = dt_end_date.strftime('%Y-%m-%d')

if __name__ == '__main__':
    # get_all_actions()
    # split_actions()
    get_simple_actions('2016-02-15','2016-02-30')

