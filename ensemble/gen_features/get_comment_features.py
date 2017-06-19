# coding=utf-8
import pandas as pd
import os
from data_path import *


def get_comment_features(end_date):
    dump_path = '../../cache/product_comments_%s.csv' % end_date
    if os.path.exists(dump_path):
        comments = pd.read_csv(dump_path)
    else:
        comments = pd.read_csv(comment_path)
        comment_date_end = end_date
        comment_date_begin = comment_date[0]
        for date in reversed(comment_date):
            if date < comment_date_end:
                comment_date_begin = date
                break
        comments = comments[(comments.dt >= comment_date_begin) & (comments.dt < comment_date_end)]
        del comments['dt']
        # del comments['comment_num']
        comments.to_csv(dump_path, index=False)
    return comments


if __name__ == '__main__':
    get_comment_features('2016-4-15')
