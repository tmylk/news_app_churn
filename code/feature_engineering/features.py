from sqlalchemy import create_engine
import pandas as pd

from helpers import conn_str, create_view

# redshift forces column and table names to be lower case
# I want to create a table/column for every event type in the future 
# In order to not think about case when doing the lookup
# I lowercase the event_types at the import stage here
def make_events_lower_case():
    view_name = "events_all"
    return create_view(view_name,
    """
    SELECT 

"Scene Card Title", domain, "channel type", "$distinct_id", "User Id", type, "Upper History Navigation", "Author Name", "Bookmark navigation", "Operating System", "Read Time", "Gallery Object Type", "$radio", os_version, "$app_release", "Screen Width", "$device", "New User", "Upper Browser", "$ip", "User Continued Authentication", mp_device_model, "Scene Title", "$manufacturer", "$os_version", screen_width, "URL", "timestamp", "$lib_version", "Upper Channel Type", "$carrier", distinct_id, "Topic ID", "Account Created", "$screen_width", e, recipient, "AccountCreated", mp_country_code, "Story URL", "Default Topic Title", "Curator Name", mp_lib, "Menu Item", "Bookmark Title", n, "Bookmark ID", t, referrer, "$app_version", "$browser", "History navigation", "Author2 Name", browser, "Number of Bookmarks", delivery_id,  "Default Topic ID", "$initial_referring_domain", "Trial to Signup", campaign_id, "External Link", "Scene ID", "Source", "$city", "Bookmark Navigation Clicked", "Curator ID", "OS Version", "Story Read Time", category, "$referrer", "Author ID", "Screen Height", "Drop Down Follow Through", "$os", "Accepted", message_id, "Time Since Story Opened", "User Created", "$wifi", "Scene Card ID", "$ios_ifa", "$email", "$initial_referrer", "Story ID", "$referring_domain", "Story Title", "$screen_height", a, "Topic Title", "Scene Card Number", "$model", "$region", "Share Source", message_type, "Story Completion", "Scene Cards", "First Launch", "Default Topic", "Result", "time", "Message", "Navigation Clicked", os, "Author2 ID", screen_height, v, id

         , LOWER(event_type) AS event_type 
    FROM raw_copy
    WHERE "time" > '2015-01-15'
    AND time < '2015-02-28 23:59:59'

    """)

def create_event_types():
    view_name = "event_types"
    return create_materialized_view(view_name,
    """SELECT DISTINCT event_type     
    FROM events_all""")

def create_stories():
    view_name = "stories"
    return create_materialized_view(view_name,
    """SELECT DISTINCT "Story ID", MAX("Story Title")
    FROM events_all
    GROUP BY "Story ID" """)

def create_users():
    view_name = "users"
    return create_view(view_name,
    """SELECT DISTINCT distinct_id, mp_country_code, "$region", "$city", "$email" 
    FROM events_all""")

def create_last_seen_view():
     return "DROP TABLE IF EXISTS last_seen_from_events CASCADE;\
    CREATE TABLE last_seen_from_events AS \
SELECT distinct_id, MAX(timestamp) AS last_seen_ts \
FROM events_all \
GROUP BY distinct_id;"

def create_first_seen_view():
    return "DROP TABLE IF EXISTS first_seen_from_events CASCADE;\
    CREATE TABLE first_seen_from_events AS \
SELECT distinct_id, MIN(timestamp) AS first_seen_ts \
FROM events_all \
GROUP BY distinct_id;"


def create_age_view():
    return "DROP TABLE IF EXISTS age_from_events CASCADE;\
    CREATE TABLE age_from_events AS \
SELECT f.distinct_id, last_seen_ts - first_seen_ts  as age_s, ceil( (last_seen_ts::numeric - first_seen_ts::numeric)/(60*60*24)) as age_days \
FROM first_seen_from_events f \
JOIN last_seen_from_events l \
ON f.distinct_id = l.distinct_id;"

# we take average over users's lifetime between first and last seen
# so need to take AVG() via join with the dates table
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)

def create_dates_table(): 
    start_date = datetime.datetime.strptime('2015-01-15',"%Y-%m-%d")
    end_date = datetime.datetime.strptime('2015-03-01',"%Y-%m-%d")
    dates_list = []
    weeks_list = []
    for single_date in daterange(start_date, end_date):    
        date = single_date.strftime("%Y-%m-%d") 
        dates_list.append(single_date)
        weeks_list.append(pd.to_datetime(single_date).week)
    df_dates = pd.DataFrame.from_dict({'date': dates_list, 'week':weeks_list})

    engine= create_engine(conn_str)
    df_dates.to_sql('dates',engine, if_exists='replace', index = False)

def create_first_last_seen_view():
    return "DROP TABLE IF EXISTS first_last_seen CASCADE;\
    CREATE TABLE first_last_seen AS \
    SELECT f.distinct_id, f.first_seen_ts, l.last_seen_ts \
    FROM first_seen_from_events f \
    JOIN last_seen_from_events l \
    ON f.distinct_id = l.distinct_id"

def create_dates_during_users_life():
    view_name = "dates_during_users_life"
    return create_view(view_name, """SELECT distinct_id, dates.date, dates.week \
        FROM dates 
        JOIN first_last_seen fl 
        ON 
            dates.date >= DATE(TIMESTAMP 'epoch' + fl.first_seen_ts * INTERVAL '1 Second ') AND 
            dates.date <= DATE(TIMESTAMP 'epoch' + fl.last_seen_ts * INTERVAL '1 Second ')
            """)

def create_weeks_during_users_life():
    view_name = "weeks_during_users_life"
    return create_materialized_view(view_name, '''SELECT distinct distinct_id, dates.week 
            FROM dates 
            JOIN first_last_seen fl 
            ON 
                dates.date >= DATE(TIMESTAMP 'epoch' + fl.first_seen_ts * INTERVAL '1 Second ') AND 
                dates.date <= DATE(TIMESTAMP 'epoch' + fl.last_seen_ts * INTERVAL '1 Second ')
                ''')

# users can have events generated as they travel around the country
def create_user_city_country():

    view_name = "user_city_country"
    return create_materialized_view(view_name,
    '''SELECT distinct_id, COUNT(1),  "$city","mp_country_code" FROM events_all
      GROUP BY distinct_id, "$city","mp_country_code"'''

)


# associate the mode of country and city with the user
def create_user_one_city_country():
    view_name = "user_one_city_country"
    return create_materialized_view(view_name,
    '''
    SELECT DISTINCT distinct_id, FIRST_VALUE("$city") OVER 
    (PARTITION BY distinct_id ) AS "$city", FIRST_VALUE("mp_country_code") OVER 
    (PARTITION BY distinct_id ) AS "mp_country_code"           
    FROM
        user_city_country
    ''')


# # for debugging
# def create_first_attempt_at_day_event_stats():
#     view_name = "first_day_event_stats"
#     return create_view(view_name,"""
#     SELECT distinct_id, AVG(day_total::real) as day_avg, SUM(day_total) as total, SUM(CASE WHEN day_total = 0 THEN 0 ELSE 1 END) as day_active  FROM day_totals
#     WHERE event_type='story completion'
#     GROUP BY distinct_id""")

# # for debugging
# def create_first_attempt_at_week_event_stats():
#     view_name = "first_week_event_stats"
#     return create_view(view_name,"""
#     SELECT distinct_id, AVG(week_total::real) as week_avg, SUM(week_total) as total, SUM(CASE WHEN week_total = 0 THEN 0 ELSE 1 END) as week_active     FROM week_totals
#     WHERE event_type='story completion'
#     GROUP BY distinct_id
#     """)



# Needed to establish what is a clean session and what is orphaned due to a bug
# Note: in resulting table there will be many rows if 
# there are multiple events on first and last second
def create_first_events():
    view_name = "first_events"
    return create_view(view_name,
        """SELECT f.distinct_id, event_type as first_event_type, 
        "new user" as first_event_new_user,result as first_event_result
        FROM first_seen_from_events f 
        JOIN events_all e 
        ON f.distinct_id = e.distinct_id
        AND f.first_seen_ts = e."timestamp"
        ;

        """)

def create_last_events():
    view_name = "last_events"
    return create_view(view_name,
       
        """SELECT *
        FROM first_seen_from_events f
        JOIN events_all e
        ON f.distinct_id = e.distinct_id
        AND   e."timestamp"  - 2*60*60  <  f.first_seen_ts
        ;

        """)


def create_users_first_last_events():
    view_name = "users_first_last_events"
    tables = get_all_first_last_event_tables()    
    return create_view(view_name,aggregate_all_features(tables)) 




# I only analyze sesssions where users have not signed up, not logged in and not used any features that force a profile creation.
# 
# I am throwing away only 15k users signed up out of 300k.
# only 2k of them logged in second time on the web.
# Same 5% proportion among users with age > 10 days.
# So assume that registration doesn't affect whether this is a power user or not.
# See cleaning_target.md
def create_clean_users():
    view_name = "clean_users"
    sql = """SELECT u.* FROM users_all_features u
    JOIN 
        (
        SELECT DISTINCT distinct_id FROM users_first_last_events WHERE 
    
        first_event_type IN 
        (
        'app viewed',
        'app navigation clicked',
        'story navigation clicked',
        'story viewed',
        'topic viewed',
        'explore topic clicked',
        'explore viewed',
        'story completion'
        )

        AND last_event_type IN
        (
        'app viewed',
        'app navigation clicked',
        'story navigation clicked',
        'story viewed',
        'topic viewed',
        'explore topic clicked',
        'explore viewed',
        'story completion'
        ) ) c
ON  c.distinct_id = u.distinct_id
WHERE
     onboard_viewed_total =0
        AND onboard_navigation_total = 0
        AND email_register_total = 0
        AND login_result_total = 0
        AND Bookmarks_Viewed_total = 0
        AND  bookmark_navigation_total =0
        AND profile_viewed_total = 0
        AND profile_page_viewed_total = 0
        AND profile_navigation_clicked_total = 0
        AND login_page_viewed_total = 0
        AND login_viewed_total = 0
        AND logging_in_user_id_total = 0
        AND bookmark_viewed_total = 0
        AND bookmark_story_total = 0
        AND bookmark_remove_total = 0
        AND forgot_password_page_viewed_total = 0
        AND bookmark_navigation_total = 0        
        ;"""
    return create_view(view_name,sql)





# not used as there are only 2k of these events for 15k registered users. Ask Eric Lin more later
# TODO: glue user sessions based on this event

# def create_logging_in_table():
#     view_name = 'logging_in_events'
#     return create_materialized_view(view_name,"""
#     SELECT 'user id' as 'user id',distinct_id as anonymous_distinct_id  FROM events_all WHERE event_type = 'Logging In: User Id';""")
# def create_glued_events():
#     view_name = "glued_events"
#     create_view(view_name,
#     """SELECT events_all.*, COALESCE(l.'user id', r.distinct_id) as user_id
#     FROM 
#     events_all r
#     LEFT JOIN logging_in_events l
#     ON l.anonymous_distinct_id = r.distinct_id""")

