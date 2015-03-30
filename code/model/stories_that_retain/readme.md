TODO: paramaterize nosetests to turn these manual steps into code or alternatively separate implementation from tests

In order to create features for "Which stories make users come back" analysis in stories_bayes.py from a raw Mixpanel import:

1) create all main features for users by running "tests_main_features"

2) create first session tables by running 'test_time_features.py' with 
c = event_features("first_session_events","frst_").

It will create the tables though the tests won't be checking anything. They are for user_ids in the main tables and they query them, not the "frst_" prefixed tables.

3) create first session tables by running 'tests_story_features.py' with 
c = event_features("first_session_events","frst_").

Same again, the test won't be checking anything. They are for event_features("events_all","") but they will create tables in correct order.

4) create more first session tables by running all_features_user_tests.test_create_users_all_story_features with c = event_features("first_session_events","frst_").

Same again, the test won't be checking anything. They are for event_features("events_all","") but they will create tables in correct order.

5) create second session tables by running test_event_features.py with c = event_features("second_day_events","scnd_")

6) create more second session tables by running test_time_features.py with c = event_features("second_day_events","scnd_") 




