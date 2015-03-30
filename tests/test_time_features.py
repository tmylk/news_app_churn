from sqlalchemy import *
import psycopg2
import nose.tools as n
import pandas as pd
from code.feature_engineering.event_features import event_features 

from tests import engine, NUM_USERS, NUM_CLEAN_USERS, execute_sql, check_one_row_per_user

c = event_features("events_all","")

#c = event_features("first_session_events","frst_")
#c = event_features("second_day_events","scnd_")

def test_create_time_to_x_day_totals():
    execute_sql(c.create_time_to_x_day_totals)
    df_test = pd.read_sql("""SELECT * FROM time_to_x_day_totals WHERE \
        distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'
        AND "story id"='4b8a78d7-9a1b-45fc-9a39-a8c7e4209569'""", engine)
    n.assert_equal(len(df_test), 5)    
    n.assert_equal(df_test[(df_test.date == "2015-02-25") &  (df_test["story completion"] == '10')]["day_time_total"].values[0], 94)
    n.assert_equal(df_test[(df_test.date == "2015-02-25") &  (df_test["story completion"] == '10')]["day_count"].values[0], 2)
    n.assert_equal(df_test[(df_test.date == "2015-02-25") &  (df_test["story completion"] == '10')]["day_time_avg"].values[0], 47)
    df_test = pd.read_sql("""SELECT distinct date FROM time_to_x_day_totals WHERE \
        distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'
        """, engine)
    n.assert_equal(len(df_test), 38) #equal to dates during users life    
#2015-02-25 00:00:00 |    9 | 681CFA45-B526-4FE4-A2DF-F73F2D0CF674 | Story Completion | 10               | 4b8a78d7-9a1b-45fc-9a39-a8c7e4209569 |        94 |         2 |      47
    


def test_create_time_to_x_day_stats():
    execute_sql(c.create_time_to_x_day_stats)
    df_test = pd.read_sql("""SELECT * FROM time_to_10_day_stats WHERE 
        distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'   AND "story id"='4b8a78d7-9a1b-45fc-9a39-a8c7e4209569'""", engine)
    n.assert_equal(len(df_test), 1)    
    n.assert_equal(round(df_test['agg_day_time_total'].values[0]), 94)
    n.assert_equal(df_test['agg_day_time_avg'].values[0], 47)
    n.assert_equal(df_test['agg_day_count'].values[0], 2)
    n.assert_equal(df_test['agg_day_count_avg'].values[0], 2) #read it in one day
 #          distinct_id              | story completion |               story id               | agg_day_total | agg_day_avg | agg_day_count
#--------------------------------------+------------------+--------------------------------------+---------------+-------------+---------------
# 681CFA45-B526-4FE4-A2DF-F73F2D0CF674 | 10               | 4b8a78d7-9a1b-45fc-9a39-a8c7e4209569 |            94 |          47 |             2


def test_create_time_to_x_agg_day_stats():
    execute_sql(c.create_time_to_x_agg_day_stats)
    df_test = pd.read_sql("""SELECT * FROM time_to_10_agg_day_stats WHERE 
        distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'""", engine)
    n.assert_equal(len(df_test), 1)
    n.assert_equal(round(df_test['agg_day_time_total'].values[0]), 4860)
    n.assert_equal(df_test['agg_day_time_avg'].values[0], 200)
    n.assert_equal(df_test['agg_day_count'].values[0], 25)
    n.assert_equal(df_test['agg_day_count_avg'].values[0], 0.6578)
    check_one_row_per_user("time_to_10_agg_day_stats")
