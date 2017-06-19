# -*- coding: utf-8 -*-
import xgboost as xgb
import numpy as np
import pandas as pd
import os
import datetime
import time
from single_GBDT import get_train_rounds
from get_train_set import processed_train_set
from get_train_set import get_test_set
from get_train_set import get_labels
from get_labels import get_evaluation_labels
from single_GBDT import report
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score
from sklearn.externals import joblib


# 取每个模型的cate==8前1K名
def get_filter_value(start_day,end_day,data_set):
    model_path = './sub/bst_%s_%s.model' %(start_day,end_day)
    if not os.path.exists(model_path):
        print "No model can be used"
        exit()
    else:

        print('Model Loading !')
        bst = xgb.Booster()  # 注意：名字要保持一致，否则报错！
        bst.load_model(model_path)

        train_set = data_set.copy()
        train_set = train_set[train_set['cate']==8]
        ui = train_set[['user_id','sku_id']].copy()
        del train_set['user_id']
        del train_set['sku_id']

        sub_train_data = xgb.DMatrix(train_set.values)
        y = bst.predict(sub_train_data)
        ui['label'] = y
        ui = ui.sort_values(by='label',ascending=False)
        ui = ui.iloc[:1000]
        actions = ui
        return actions


# 组合每个模型的前1K名
def get_lr_train_data(train_start,train_end):
    dump_path = '../../cache/lr_train_%s_%s.csv' %(train_start,train_end)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        train_data = None
        nums, rounds = get_train_rounds(45, 3)
        counter = 0
        ui, train_set, label = processed_train_set(train_start, train_end)
        # train_set = None
        for this_round in rounds:
            if 0 <= counter < 6:
                print this_round
                action_unit = get_filter_value(this_round[0],this_round[1],train_set)
                if train_data is None:
                    train_data = action_unit
                else:
                    train_data = pd.merge(train_data,action_unit,how='outer',on=['user_id','sku_id'])
            counter += 1
        actions = train_data
        actions.to_csv(dump_path, index=False)
    return actions


# 处理它
def processed_lr_data_set(train_start,train_end):
    dump_path = '../../cache/processed_lr_train_%s_%s.csv' %(train_start,train_end)
    if os.path.exists(dump_path):
        actions = pd.read_csv(dump_path)
    else:
        ui = get_lr_train_data(train_start,train_end)

        datetype_test_end_date = datetime.datetime.strptime(train_end, '%Y-%m-%d')
        labels_end_date = (datetype_test_end_date + datetime.timedelta(days=5)).strftime('%Y-%m-%d')
        labels = get_labels(train_end, labels_end_date)

        ui = pd.merge(ui,labels,how='left',on=['user_id','sku_id'])

        actions = ui.fillna(0)
        actions.to_csv(dump_path, index=False)
    return actions


def lr_train(train_start,train_end):
    model_path = "./sub/lr_model_%s_%s" % (train_start,train_end)
    if not os.path.exists(model_path):
        data = processed_lr_data_set(train_start, train_end)
        ui = data.copy()[['user_id', 'sku_id']]
        label = data.copy()['label']
        training_data = data
        del training_data['user_id']
        del training_data['sku_id']
        del training_data['label']
        # x_train, x_test, y_train, y_test = train_test_split(training_data.values, label.values, test_size=0.2,
        #                                                     random_state=0)
        classifier = LogisticRegression(penalty='l2', class_weight='balanced')  # 使用类，参数全是默认的
        classifier.fit(training_data.values, label.values)  # 训练数据来学习，不需要返回值
        # score_p = cross_val_score(classifier, training_data.values, label.values, cv=5, scoring='roc_auc')
        # score_r = cross_val_score(classifier, training_data.values, label.values, cv=5, scoring='recall')
        # print score_p,score_r
        joblib.dump(classifier, model_path)


def lr_evaluation():
    test_start_date = '2016-02-19'
    test_end_date = '2016-04-04'

    test_start_label = '2016-04-04'
    test_end_label = '2016-04-09'

    # 训练好的模型 时间为训练数据的时间
    lr_train('2016-02-22', '2016-04-07')
    model_path = "./sub/lr_model_%s_%s" % ('2016-02-22', '2016-04-07')

    # 拿到评测用的数据
    evaluation = processed_lr_data_set(test_start_date,test_end_date)
    ui = evaluation.copy()[['user_id','sku_id']]
    del evaluation['user_id']
    del evaluation['label']
    del evaluation['sku_id']

    evaluation_true = get_evaluation_labels(test_start_label,test_end_label)
    del evaluation_true['label']

    clf = joblib.load(model_path)

    y = clf.predict_proba(evaluation)
    ui['label'] = pd.Series(y[:,1])

    ui = ui.sort_values(by='label', ascending=False)
    for i in range(300,900,50):
        this = ui.copy().iloc[:i]
        this = this.groupby(['user_id'],as_index=False).apply(lambda x: x[x['label'] == x['label'].max()])
        this = this[['user_id','sku_id']]
        print i
        report(this, evaluation_true)


def xgboost_evaluation():
    test_start_date = '2016-02-19'
    test_end_date = '2016-04-04'

    test_start_label = '2016-04-04'
    test_end_label = '2016-04-09'

    # 训练好的模型 时间为训练数据的时间
    # lr_train('2016-02-22', '2016-04-07')
    model_path = './sub/bst_%s_%s.model' % ('2016-02-22', '2016-04-07')

    # 拿到评测用的数据
    evaluation = get_test_set(test_start_date,test_end_date)
    ui = evaluation.copy()[['user_id','sku_id']]
    del evaluation['user_id']
    del evaluation['label']
    del evaluation['sku_id']

    evaluation_true = get_evaluation_labels(test_start_label,test_end_label)
    del evaluation_true['label']

    if os.path.exists(model_path):
        print('Model Loading !')
        bst = xgb.Booster()  # 注意：名字要保持一致，否则报错！
        bst.load_model(model_path)
    else:
        print "No model can be used"
        exit()

    sub_trainning_data = xgb.DMatrix(evaluation.values)
    y = bst.predict(sub_trainning_data)
    ui['label'] = y

    ui = ui.sort_values(by='label', ascending=False)
    for i in range(300,900,50):
        this = ui.copy().iloc[:i]
        this = this.groupby(['user_id'],as_index=False).apply(lambda x: x[x['label'] == x['label'].max()])
        this = this[['user_id','sku_id']]
        print i
        report(this, evaluation_true)



if __name__ == '__main__':
    # dump_path = '../../cache/processed_lr_train_%s_%s.csv' %('2016-02-22','2016-04-07')
    # dump_path1 = '../../cache/processed_train_set_2016-02-22_2016-04-07.csv'
    # get_lr_train_data('2016-02-22','2016-04-07')
    # processed_lr_data_set('2016-02-19','2016-04-04')
    # lr_train('2016-02-22','2016-04-07')
    # lr_evaluation('2016-02-19','2016-04-04')
    xgboost_evaluation()
# def xgboost_submisson(level_set=None):
#     sub_start_date = '2016-04-01'
#     sub_end_date = '2016-04-16'
#
#     model_path = './sub/bst.model'
#     if os.path.exists(model_path):
#         print('Model Loading !')
#         bst = xgb.Booster()  # 注意：名字要保持一致，否则报错！
#         bst.load_model(model_path)
#     else:
#         print 'Error No module'
#         exit()
#     print("--------------生成提交结果-----------")
#     sub_user_index, sub_trainning_data = make_submit_set(sub_start_date, sub_end_date, accdays=[3, 5, 7, 15])
#     sub_trainning_data = xgb.DMatrix(sub_trainning_data.values)
#     y = bst.predict(sub_trainning_data)
#     sub_user_index['label'] = y
#     if level_set is None:
#         level_set = label_level
#     for lv in level_set:
#         output = './sub/submission_%s.csv' % (str(lv))
#         if os.path.exists(output):
#             continue
#         pred = sub_user_index[sub_user_index['label'] >= lv]
#         pred = pred.groupby('user_id', as_index=False).apply(get_the_best_one).reset_index(drop=True)
#         pred = pred[['user_id', 'sku_id']]
#         # sub_user_index 中的 userid 可能会对应多个skuid，所以需要多userid做group操作，取score最大的sku ?
#         pred['user_id'] = pred['user_id'].astype(int)
#         pred.to_csv(output, index=False, index_label=False)
#     print("--------------结果生成————-----------")
#
# def lr_train(train_start,train_end):


# --------------------------------