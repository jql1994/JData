# -*- coding: utf-8 -*-
from get_train_set import processed_train_set
from sklearn.model_selection import train_test_split
import xgboost as xgb
import numpy as np
import pandas as pd
import datetime
import time
import os


label_level = [0.03,0.04,0.05,0.2, 0.3, 0.4, 0.42, 0.44, 0.46, 0.48, 0.5, 0.52,
               0.54, 0.56, 0.58, 0.6, 0.62, 0.64, 0.66, 0.68, 0.7]


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


def report(pred, label):
    actions = label
    result = pred

    # 所有用户商品对
    all_user_item_pair = actions['user_id'].map(str) + '-' + actions['sku_id'].map(str)
    all_user_item_pair = np.array(all_user_item_pair)
    # 所有购买用户
    all_user_set = actions['user_id'].unique()

    # 所有品类中预测购买的用户
    all_user_test_set = result['user_id'].unique()
    all_user_test_item_pair = result['user_id'].map(str) + '-' + result['sku_id'].map(str)
    all_user_test_item_pair = np.array(all_user_test_item_pair)

    # 计算所有用户购买评价指标
    pos, neg = 0, 0
    for user_id in all_user_test_set:
        if user_id in all_user_set:
            pos += 1
        else:
            neg += 1
    all_user_acc = 1.0 * pos / (pos + neg)
    all_user_recall = 1.0 * pos / len(all_user_set)
    print '所有用户中预测购买用户的准确率为 ' + str(all_user_acc)
    print '所有用户中预测购买用户的召回率' + str(all_user_recall)

    pos, neg = 0, 0
    for user_item_pair in all_user_test_item_pair:
        if user_item_pair in all_user_item_pair:
            pos += 1
        else:
            neg += 1
    try:
        all_item_acc = 1.0 * pos / (pos + neg)
        all_item_recall = 1.0 * pos / len(all_user_item_pair)
        F11 = 6.0 * all_user_recall * all_user_acc / (5.0 * all_user_recall + all_user_acc)
        F12 = 5.0 * all_item_acc * all_item_recall / (2.0 * all_item_recall + 3 * all_item_acc)
        print '所有用户中预测购买商品的准确率为 ' + str(all_item_acc)
        print '所有用户中预测购买商品的召回率' + str(all_item_recall)
        score = 0.4 * F11 + 0.6 * F12
        print 'F11=' + str(F11)
        print 'F12=' + str(F12)
        print 'score=' + str(score)
    except Exception, e:
        print Exception, ":", e
        return 1
    return 0


def xgboost_train(start_day,end_day):
    model_path = './sub/bst_%s_%s.model' %(start_day,end_day)
    # print model_path
    # exit()
    user_index, training_data, label = processed_train_set(start_day,end_day)
    # user_index, training_data, label = data_set()
    # user_index, training_data, label = make_train_set(train_start_date, train_end_date)
    # 从样本中随机的按比例选取train data和test data。test_size是样本占比，如果是整数的话就是样本的数量。random_state是随机数的种子。
    x_train, x_test, y_train, y_test = train_test_split(training_data.values, label.values, test_size=0.2,
                                                        random_state=0)
    # 下面这一部分是为了抽取 x_test的user index 测准确度
    x_train_df = pd.DataFrame(x_train)
    x_test_df = pd.DataFrame(x_test)
    # x_test_df.to_csv('./sub/x_test.csv', index=False, index_label=False)
    x_train = x_train_df.iloc[:, 2:].copy()
    test_index = x_test_df.iloc[:, [0, 1]].copy()
    test_index.columns = ['user_id', 'sku_id']
    x_test = x_test_df.iloc[:, 2:].copy()

    del training_data['user_id']
    del training_data['sku_id']

    if os.path.exists(model_path):
        print('Model Loading !')
        bst = xgb.Booster()  # 注意：名字要保持一致，否则报错！
        bst.load_model(model_path)
    else:
        dtrain = xgb.DMatrix(x_train.values, label=y_train)
        dtest = xgb.DMatrix(x_test.values, label=y_test)
        # 'max_delta_step':1
        # param = {'learning_rate': 0.15, 'n_estimators': 1000, 'max_depth': 3, 'max_delta_step': 1,
        #          'min_child_weight': 5, 'gamma': 0, 'subsample': 1.0, 'colsample_bytree': 0.8,
        #          'scale_pos_weight': 1, 'eta': 0.05, 'silent': 1, 'objective': 'binary:logistic'}
        # param = {'n_estimators': 1000, 'max_depth': 3, 'max_delta_step': 1,
        #          'min_child_weight': 5, 'gamma': 0, 'subsample': 1.0, 'colsample_bytree': 0.8,
        #          'scale_pos_weight': 13, 'eta': 0.05, 'silent': 1, 'objective': 'binary:logistic'}
        param = {'learning_rate': 0.15, 'n_estimators': 1000, 'max_depth': 3, 'max_delta_step': 1,
                 'min_child_weight': 5, 'gamma': 0, 'subsample': 1.0, 'colsample_bytree': 0.8,
                 'scale_pos_weight': 1, 'eta': 0.05, 'silent': 1, 'objective': 'binary:logistic'}

        # num_round 训练轮数
        # num_round = 290
        num_round = 290
        # 并行度，用几个cpu
        # param['nthread'] = 6
        # param['eval_metric'] = "auc"
        # items() 返回键值对
        plst = param.items()
        plst += [('eval_metric', 'auc')]
        # evallist是验证集，这里用测试集和训练集做两次验证
        evallist = [(dtest, 'eval'), (dtrain, 'train')]
        bst = xgb.train(plst, dtrain, num_round, evallist)
        bst.save_model(model_path)

    print("-------------- 测试集准确度-----------")
    # 测试集实际
    test_index['label'] = y_test
    test_true = test_index[test_index['label'] == 1]
    test_true = test_true[['user_id', 'sku_id']]
    # test_true.to_csv('./sub/testtrue1.csv', index=False, index_label=False)
    # 会有用户购买多个sku，但是最终结果每个用户只能买一个商品，问题是多个商品里面取哪一个？
    # test_true = test_true.first().reset_index(drop=True)
    # test_true.to_csv('./sub/testtrue2.csv', index=False, index_label=False)
    del test_index['label']

    # 测试集预测
    test_input = xgb.DMatrix(x_test.values)
    test_index['label'] = bst.predict(test_input)

    x_test['label'] = y_test
    for lv in label_level:
        print("this Probality = " + str(lv))
        test_pred = test_index[test_index['label'] >= lv]
        test_pred = test_pred[['user_id', 'sku_id']]
        test_pred = test_pred.groupby('user_id', as_index=False).first().reset_index(drop=True)
        res = report(test_pred, test_true)
        if res == 1:
            break
        print("")


# def xgboost_evaluation():
#     test_start_date = '2016-03-24'
#     test_end_date = '2016-04-08'
#
#     model_path = './sub/bst.model'
#
#     evaluation = make_test_set(test_start_date, test_end_date)
#     evaluation_true = evaluation[evaluation['label'] == 1]
#     # 测试集真实值
#     evaluation_true = evaluation_true[['user_id', 'sku_id']]
#     sub_user_index = evaluation[['user_id', 'sku_id']].copy()
#     del evaluation['label']
#     del evaluation['user_id']
#     del evaluation['sku_id']
#
#     if os.path.exists(model_path):
#         print('Model Loading !')
#         bst = xgb.Booster()  # 注意：名字要保持一致，否则报错！
#         bst.load_model(model_path)
#     else:
#         print "No model can be used"
#
#     # 构建预测集合
#     print("--------------测试结果-----------")
#     sub_trainning_data = xgb.DMatrix(evaluation.values)
#     y = bst.predict(sub_trainning_data)
#     sub_user_index['label'] = y
#
#     for lv in label_level:
#         print("this Probility = " + str(lv))
#         test_pred = sub_user_index[sub_user_index['label'] >= lv]
#         # test_pred = test_pred.groupby('user_id', as_index=False).apply(get_the_best_one).reset_index(drop=True)
#         # test_pred = test_pred[['user_id', 'sku_id']]
#         test_pred = test_pred[['user_id', 'sku_id']]
#         test_pred = test_pred.groupby('user_id', as_index=False).first().reset_index(drop=True)
#         res = report(test_pred, evaluation_true)
#         if 1 == res:
#             break
#         print("")


if __name__ == '__main__':
    # print get_decay(range(1,10))
    nums,rounds = get_train_rounds(45,3)
    counter = 0
    for this_round in rounds:
        if 7 <= counter < 8:
            print this_round[0],this_round[1]
            xgboost_train(this_round[0],this_round[1])
        counter +=1
