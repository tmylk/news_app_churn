import features
from helpers import create_view

FIRST_SESSION_HOURS = 24

# needed to define the first visit
# (how many hours long it is )
# see model/stories_retain.py
def create_first_100_completion_view():
    return """DROP TABLE IF EXISTS first_100_completion CASCADE;
    CREATE TABLE first_100_completion AS 
SELECT distinct_id, MIN(timestamp) AS first_100_completion_ts 
FROM events_all 
WHERE
event_type = 'story completion'
AND
"story completion"='100'

GROUP BY distinct_id;"""


def create_first_seen_first_completion_view():
    return """DROP TABLE IF EXISTS first_seen_first_completion CASCADE;  
     CREATE TABLE first_seen_first_completion AS 
    SELECT f.distinct_id, f.first_seen_ts, c.first_100_completion_ts 
    FROM first_seen_from_events f 
    LEFT JOIN first_100_completion c 
    ON f.distinct_id = c.distinct_id;"""


def cut_events_by_time(view_name, condition):       
    main_table = "e"
    columns = get_all_columns_without_id("events_all")

    col_sql = main_table +".distinct_id, "
    for col in columns:
        col_sql += main_table+"."+"\""+col +"\""+ ", "
    col_sql = col_sql[:-2]

    return create_view(view_name,
        """SELECT """ + col_sql +"""FROM first_seen_from_events f
        JOIN events_all e
        ON f.distinct_id = e.distinct_id
        AND   """ + condition +  """
        ;

        """)


# events that happened in first  hours since user first seen 
# needed to find what makes users stay on first visit
def create_first_session_events():
    view_name = "first_session_events"
    condition = """e."timestamp"  - """ +str(FIRST_SESSION_HOURS) + """*60*60  <  f.first_seen_ts"""
    return cut_events_by_time(view_name, condition)


# can tell us if user ever came back and if stayed engaged then
def create_second_day_events():
    view_name = "second_day_events"
    condition = """e."timestamp"  >  f.first_seen_ts + """ +str(FIRST_SESSION_HOURS) + """*60*60"""
    return cut_events_by_time(view_name, condition)


# only use clean users in the analysis
# a clean user is a global condition, need all events to decide
# looking at first and second day separately is not sufficient,
# so need to join with clean_user generated from the raw import of all events

def get_filter_clean_users_sql(table):
    ame = "clean_users"
    sql = """SELECT u.* FROM """ + table + """ u
    JOIN clean_users c
    ON c.distinct_id = u.distinct_id
    """

def create_clean_first_session_stories():    
    view_name = "clean_first_session_stories"
    sql = get_sql_clean_users("frst_users_all_stories")

def create_clean_second_day_events():    
    view_name = "clean_first_session_stories"
    sql = get_sql_clean_users("frst_users_all_stories")

