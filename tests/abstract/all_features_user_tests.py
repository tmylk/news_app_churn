
from sqlalchemy import *
import psycopg2
import nose.tools as n
import pandas as pd
from code.feature_engineering.event_features import event_features 

from tests.helpers import engine, NUM_USERS, NUM_CLEAN_USERS, execute_sql, check_one_row_per_user
import tests

from unittest import TestCase

class AllFeaturesUserTestsBaseClass(TestCase):
    # abstract 
    c = None
     __test__ = False


    NUM_STORIES = 207
    NUM_COLS_ALL_FEATURES=56*3 + 5*4 +1*5+1+2

 
    def test_create_users_all_static_features():
        create_agg_feat_table(c.create_users_all_static_features, "users_all_static_features", 8)
        

    def test_create_users_all_time_features():
        create_agg_feat_table(c.create_users_all_time_features, "users_all_time_features", 5*4 +1)
        
        
    def create_agg_feat_table(method, table, num_cols):
        execute_sql(method)   
        
        df_test = pd.read_sql("""SELECT * FROM """+ table +""" WHERE 
                distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'""", engine)
        n.assert_equal(len(df_test), 1)
        n.assert_equal(len(df_test.columns), num_cols)
        check_one_row_per_user(table)



    def test_create_users_users_all_event_features_1():
        create_agg_feat_table(c.create_users_all_event_features_1, "users_all_event_features_1", 28*3 +1)
        
    def test_create_users_users_all_event_features_2():
        create_agg_feat_table(c.create_users_all_event_features_2, "users_all_event_features_2", 28*3 +1)


    def test_create_users_all_features():
         create_agg_feat_table(c.create_users_all_features, "users_all_features",NUM_COLS_ALL_FEATURES)



    def test_create_users_all_story_features():

        first = 0
        last = 0

        chunk_size = 30
        for i in  range(NUM_STORIES /chunk_size +1):
            first = i * chunk_size
            last =  (i + 1) *chunk_size

            if last > NUM_STORIES:
                last = NUM_STORIES
            

            sql = c.create_users_all_story_features(i, first, last)
            sql = tests.replace_dbs(sql)
            engine.execute(sql)

            table = "users_all_story_features_"+str(i)
            df_test = pd.read_sql("""SELECT * FROM """+ table +""" WHERE 
                    distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'""", engine)
            n.assert_equal(len(df_test), 1)
            n.assert_equal(len(df_test.columns), (last - first)*1 +1)
            check_one_row_per_user(table)

    def test_create_users_all_stories():
         create_agg_feat_table(c.create_users_all_stories, "users_all_stories",NUM_STORIES*1 + 1+7)
        
