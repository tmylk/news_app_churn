from sqlalchemy import execute_sql
import psycopg2
import nose.tools as n
import pandas as pd
from code.feature_engineering.event_features import event_features

from tests import engine, NUM_USERS, NUM_CLEAN_USERS, execute_sql, check_one_row_per_user
from unittest import TestCase


class EventFeaturesTestBaseClass(TestCase):
    c = None
        __test__ = False  # to tell nose this is an abstract test class

    def test_create_day_totals():
    execute_sql(c.create_day_totals)
    df_test = pd.read_sql("SELECT * FROM day_totals WHERE \
        distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'", engine)
    n.assert_equal(len(df_test), 56 * 38)
    n.assert_equal(
        df_test[
            (df_test.date == "2015-01-22") & (
                df_test.event_type == "story completion")]["day_total"].values[0],
        4)

    def test_create_all_day_event_stats():
    execute_sql(c.create_all_day_event_stats)
    df_test = pd.read_sql("""SELECT * FROM story_completion_day_stats WHERE
        distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'""", engine)
    n.assert_equal(len(df_test), 1)
    n.assert_equal(round(df_test['day_avg'].values[0]), 4)
    n.assert_equal(df_test['total'].values[0], 148)
    n.assert_equal(df_test['day_active'].values[0], 21)
    check_one_row_per_user("story_completion_day_stats")

    def test_create_all_agg_day_event_stats():
    execute_sql(c.create_all_agg_day_event_stats)
    df_test = pd.read_sql(
        """SELECT * FROM Story_Completion_agg_day_stats""",
        engine)
    n.assert_equal(len(df_test), 45)
    # 2015-01-22  4   2015-01-22  1.153288    90141   15981
    df_test = df_test[df_test.date == '2015-01-22']
    n.assert_equal(round(df_test['agg_day_avg'].values[0]), 1)
    n.assert_equal(df_test['agg_day_total'].values[0], 90141)
    n.assert_equal(df_test['agg_day_unique'].values[0], 15981)

    def test_create_week_totals():
    execute_sql(c.create_week_totals)
    df_test = pd.read_sql("SELECT * FROM week_totals WHERE \
        distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'", engine)
    n.assert_equal(len(df_test), 56 * 6)
    n.assert_equal(df_test['week'].min(), 4)
    n.assert_equal(df_test['week'].max(), 9)
    n.assert_equal(
        df_test[
            (df_test.week == 4) & (
                df_test.event_type == "story completion")]["week_total"].values[0],
        29)

    def test_create_all_week_event_stats():
    execute_sql(c.create_all_week_event_stats)
    df_test = pd.read_sql("""SELECT * FROM Story_Completion_week_stats WHERE
        distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'""", engine)

    #        distinct_id              |      week_avg       | total | week_active
    #--------------------------------------+---------------------+-------+----
    # 681CFA45-B526-4FE4-A2DF-F73F2D0CF674 | 24.6666666666666667 |   148 |
    # 6
    n.assert_equal(len(df_test), 1)
    n.assert_equal(round(df_test['week_avg'].values[0]), 25)
    n.assert_equal(df_test['total'].values[0], 148)
    n.assert_equal(df_test['week_active'].values[0], 6)
    check_one_row_per_user("Story_Completion_week_stats")

    # not used as there is only one event per anonymous user somehow.
    # Github issue raised
    # def test_create_logging_in_table():
    #     execute_sql(c.create_logging_in_table)
    #     df_test = pd.read_sql("""SELECT * FROM logging_in_events""", engine)
    #     n.assert_equal(len(df_test), 1812)

    # def test_create_glued_events():
    #     execute_sql(c.create_glued_events)
    #     df_test = pd.read_sql("""SELECT * FROM glued_events WHERE
    #         distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'""", engine)
    #     n.assert_equal(len(df_test), 9860)
