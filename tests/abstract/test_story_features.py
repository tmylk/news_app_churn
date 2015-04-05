from sqlalchemy import *
import psycopg2
import nose.tools as n
import pandas as pd
from code.feature_engineering.event_features import event_features

from tests import engine, NUM_USERS, NUM_CLEAN_USERS, execute_sql, check_one_row_per_user

import tests
from all_features_user_tests import create_agg_feat_table
from unittest import TestCase


class StoryFeaturesTestBaseClass(TestCase):
    c = None
        __test__ = False  # to tell nose this is an abstract test class


NUM_STORIES = 207


def test_create_all_day_story_stats():
    execute_sql(c.create_all_day_story_stats)
    df_test = pd.read_sql("""SELECT * FROM story_4368f127_e34b_44d7_807e_5f1a72638a79_day_stats WHERE
        distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'
        """, engine)
    n.assert_equal(len(df_test), 1)
    n.assert_equal(round(df_test['agg_day_time_total'].values[0]), 3000)
    n.assert_equal(df_test['agg_day_time_avg'].values[0], 3000)
    n.assert_equal(df_test['agg_day_count'].values[0], 1)
    n.assert_equal(df_test['agg_day_count_avg'].values[0], 1)
    check_one_row_per_user(
        "story_4368f127_e34b_44d7_807e_5f1a72638a79_day_stats")
#           distinct_id              | story completion |               story id               | agg_day_time_total | agg_day_time_avg | agg_day_count | agg_day_count_avg
# --------------------------------------+------------------+--------------------------------------+--------------------+------------------+---------------+-------------------
# 681CFA45-B526-4FE4-A2DF-F73F2D0CF674 | 100              |
# 4368f127-e34b-44d7-807e-5f1a72638a79 |               3000 |
# 3000 |             1 |                 1

# TODO: move most of this procedure to code .
# write test that iterates through a table for every story like this code does
# separate code and implementation!


def test_create_users_all_story_features():

    first = 0
    last = 0

    chunk_size = 30
    for i in range(NUM_STORIES / chunk_size + 1):
        first = i * chunk_size
        last = (i + 1) * chunk_size

        if last > NUM_STORIES:
            last = NUM_STORIES

        sql = c.create_users_all_story_features(i, first, last)
        sql = tests.replace_dbs(sql)
        engine.execute(sql)

        table = "users_all_story_features_" + str(i)
        df_test = pd.read_sql("""SELECT * FROM """ + table + """ WHERE
                distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'""", engine)
        n.assert_equal(len(df_test), 1)
        n.assert_equal(len(df_test.columns), (last - first) * 1 + 1)
        check_one_row_per_user(table)


def test_create_users_all_stories():
    create_agg_feat_table(
        c.create_users_all_stories,
        "users_all_stories",
        NUM_STORIES * 1 + 1 + 7)
