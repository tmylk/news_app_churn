from sqlalchemy import *
import psycopg2
import nose.tools as n
import pandas as pd
from code.feature_engineering import features as c
from helpers import NUM_CLEAN_USERS, NUM_USERS, NUM_COLS_ALL_FEATURES

# All events are used in these tests


def test_one():
    df = pd.read_sql("SELECT * FROM raw_copy LIMIT 1", engine)
    n.assert_equal(len(df), 1)


def test_make_events_lower_case():
    execute_sql(c.make_events_lower_case)
    df_test = pd.read_sql("SELECT * FROM events_all WHERE \
        distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674' \
        AND event_type='app viewed'", engine)
    n.assert_equal(len(df_test), 70)


def test_create_events():
    execute_sql(c.create_events)
    df_test = pd.read_sql("SELECT * FROM event_types", engine)
    n.assert_equal(len(df_test), 56)


def test_create_stories():
    execute_sql(c.create_stories)
    df_test = pd.read_sql("SELECT * FROM stories", engine)
    n.assert_equal(len(df_test), 207)


def test_last_seen():
    execute_sql(c.create_last_seen_view)
    df_test = pd.read_sql("SELECT * FROM last_seen_from_events WHERE \
        distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'", engine)
    n.assert_equal(len(df_test), 1)
    n.assert_equal(df_test['last_seen_ts'][0], 1425163650)


def test_first_seen():
    execute_sql(c.create_first_seen_view)
    df_test = pd.read_sql("SELECT * FROM first_seen_from_events WHERE \
        distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'", engine)
    n.assert_equal(len(df_test), 1)
    n.assert_equal(df_test['first_seen_ts'][0], 1421924983)
    check_one_row_per_user("first_seen_from_events")


def test_age():
    execute_sql(c.create_age_view)
    df_test = pd.read_sql("SELECT * FROM age_from_events WHERE \
        distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'", engine)
    n.assert_equal(len(df_test), 1)
    n.assert_equal(df_test['age_days'][0], 38)
    check_one_row_per_user("age_from_events")


def test_first_last_seen_view():
    execute_sql(c.create_first_last_seen_view)
    df_test = pd.read_sql("SELECT * FROM first_last_seen WHERE \
        distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'", engine)
    n.assert_equal(len(df_test), 1)
    n.assert_equal(df_test['first_seen_ts'][0], 1421924983)
    n.assert_equal(df_test['last_seen_ts'][0], 1425163650)


def test_create_dates_during_users_life():
    execute_sql(c.create_dates_during_users_life)
    df_test = pd.read_sql("SELECT * FROM dates_during_users_life WHERE \
        distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'", engine)
    n.assert_equal(len(df_test), 38)


def test_create_weeks_during_users_life():
    execute_sql(c.create_weeks_during_users_life)
    df_test = pd.read_sql("SELECT * FROM weeks_during_users_life WHERE \
        distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'", engine)
    n.assert_equal(len(df_test), 6)
    n.assert_equal(df_test['week'].min(), 4)
    n.assert_equal(df_test['week'].max(), 9)


def test_create_user_city_country():

    execute_sql(c.create_user_city_country)
    df_test = pd.read_sql("""SELECT * FROM user_city_country WHERE
        distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'""", engine)
    n.assert_equal(len(df_test), 2)

    n.assert_equal(
        df_test[
            (df_test['$city'] == 'Fremont')]["count"].values[0],
        2)


def test_create_user_one_city_country():

    execute_sql(c.create_user_one_city_country)
    df_test = pd.read_sql("""SELECT * FROM user_one_city_country WHERE
        distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'""", engine)
    n.assert_equal(len(df_test), 1)
    n.assert_equal(
        df_test[
            (df_test['$city'] == 'Berkeley')]["mp_country_code"].values[0],
        "US")
    check_one_row_per_user("user_one_city_country")


def test_create_last_events():
    table = "last_events"

    execute_sql(c.create_last_events)
    df_test = pd.read_sql("""SELECT * FROM """ + table + """ WHERE
            distinct_id = '0148AF07-D0D9-4534-BD71-4351C85977C4'""", engine)
    n.assert_equal(len(df_test), 1)
    n.assert_equal(len(df_test.columns), 4)

#     timestamp  |    event_type    |        time
# ------------+------------------+---------------------
#  1425111424 | story completion | 2015-02-28 03:17:04
    n.assert_equal(df_test.last_event_type.values[0], 'story completion')

    cnt = engine.execute("SELECT COUNT(1) FROM " + table).scalar()
    n.assert_true(cnt >= NUM_USERS)


def test_create_first_events():
    table = "first_events"
    execute_sql(c.create_first_events)

    df_test = pd.read_sql("""SELECT * FROM """ + table + """ WHERE
            distinct_id = '47b8ff73-433d-4fb9-ad5c-798a8a52fcba'""", engine)
    n.assert_equal(len(df_test), 1)
    n.assert_equal(len(df_test.columns), 4)

#  os |    $os    | $ip |             distinct_id              |  event_type  | new user | result  | first_seen_ts
# ----+-----------+-----+--------------------------------------+--------------+----------+---------+---------------
#     | iPhone OS |     | 47b8ff73-433d-4fb9-ad5c-798a8a52fcba | login result | true     | Success |    1421746187
    n.assert_equal(df_test.first_event_type.values[0], 'login result')
    cnt = engine.execute("SELECT COUNT(1) FROM " + table).scalar()
    n.assert_true(cnt >= NUM_USERS)


def test_create_first_session_events():
    table = "first_session_events"

    execute_sql(c.create_first_session_events)
    df_test = pd.read_sql("""SELECT * FROM """ + table + """ WHERE
            distinct_id = '0148AF07-D0D9-4534-BD71-4351C85977C4'""", engine)
    n.assert_equal(len(df_test), 95)
    n.assert_equal(len(df_test.columns), 108)


def test_create_second_day_events():
    table = "second_day_events"

    execute_sql(c.create_second_day_events)
    df_test = pd.read_sql("""SELECT * FROM """ + table + """ WHERE
            distinct_id = '0148AF07-D0D9-4534-BD71-4351C85977C4'""", engine)
    n.assert_equal(len(df_test), 9765)
    n.assert_equal(len(df_test.columns), 108)


def test_create_users_first_last_events():
    table = "users_first_last_events"

    execute_sql(c.create_users_first_last_events)
    df_test = pd.read_sql("""SELECT * FROM """ + table + """ WHERE
            distinct_id = '0148AF07-D0D9-4534-BD71-4351C85977C4'""", engine)
    n.assert_equal(len(df_test), 1)
    n.assert_equal(len(df_test.columns), 7)

#     timestamp  |    event_type    |        time
# ------------+------------------+---------------------
#  1425111424 | story completion | 2015-02-28 03:17:04
    n.assert_equal(df_test.last_event_type.values[0], 'story completion')

    cnt = engine.execute("SELECT COUNT(1) FROM " + table).scalar()
    n.assert_true(cnt >= NUM_USERS)


def test_create_clean_users():
    table = "clean_users"
    execute_sql(c.create_clean_users)
    df_test = pd.read_sql("""SELECT * FROM """ + table + """ WHERE
            distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'""", engine)
    n.assert_equal(len(df_test), 1)
    n.assert_equal(len(df_test.columns), NUM_COLS_ALL_FEATURES)

#     timestamp  |    event_type    |        time
# ------------+------------------+---------------------
#  1425111424 | story completion | 2015-02-28 03:17:04

    cnt = engine.execute("SELECT COUNT(1) FROM " + table).scalar()
    n.assert_equal(cnt, NUM_CLEAN_USERS)


def test_create_first_session_clean_users_stories():
    table = "frst_clean_users_stories"
    execute_sql(c.create_first_session_clean_users)
    df_test = pd.read_sql("""SELECT * FROM """ + table + """ WHERE
            distinct_id = '681CFA45-B526-4FE4-A2DF-F73F2D0CF674'""", engine)
    n.assert_equal(len(df_test), 1)
    n.assert_equal(len(df_test.columns), NUM_COLS_ALL_FEATURES)

#     timestamp  |    event_type    |        time
# ------------+------------------+---------------------
#  1425111424 | story completion | 2015-02-28 03:17:04

    cnt = engine.execute("SELECT COUNT(1) FROM " + table).scalar()
    n.assert_equal(cnt, NUM_CLEAN_USERS)


def teardown():
    engine.dispose()
