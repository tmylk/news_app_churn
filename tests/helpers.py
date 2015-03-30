import nose.tools as n
import pandas as pd
from code.feature_engineering import features as c


NUM_USERS=307778
NUM_CLEAN_USERS = 209446


engine = create_engine("redshift+psycopg2://levkonst:****.redshift.amazonaws.com:5439/**db")
test_tables = {'raw_copy':'raw_copy_test'}


#useful when running quick check without creating anything
create_nothing = False

# substitute table names if wish to test on a small subsample
def replace_dbs(sql):
#    for k, v in test_tables.iteritems():
#        sql = sql.replace(k,v)
    return sql

def execute_sql(method):
    if create_nothing:
        print "Just check without Creating tables mode is ON"
        return

    sql = method()
    sql = replace_dbs(sql)
    engine.execute(sql)


def check_one_row_per_user(table):
    cnt=engine.execute("SELECT COUNT(1) FROM "+table).scalar()
    n.assert_equal(cnt, NUM_USERS)


