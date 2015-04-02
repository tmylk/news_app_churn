
# How to create features for "Which stories make users come back" analysis in stories_bayes.py from a raw Mixpanel import

1) Export from Mixpanel with mixpanel_import/import.py

2) import the csv into Redshift table "raw_copy"

3) Create features
Run in bash:

nosetests tests:core:tests_main_features
nosetests tests:first_session:test_time_features
nosetests tests:first_session:tests_story_features
nosetests tests:first_session:all_feature_user_tests.test_create_users_all_story_features
nosetests tests:second_session:test_event_features
nosetests tests:second_session:test_time_features

Note:
Tests passing/failing is a bit meaningless here in all files except first one. See explanation below.

I wrote tests as I was writing SQL queries to extract features from the table 
containing all events "events_all". The order in which the tables need to be created
is recorded in the order in which the tests are to be run. 

Later I needed to run the same SQL queries on subsets of the events - "first session" 
and "second session". A quick way was to inherit the "events_all" tests to create them.

The files in tests.first_session and tests.second_session will create the tables but the tests there won't be checking anything. The user_ids and table names in the asserts is for the main tables not the "frst_" or "scnd_" prefixed tables. However they will create "frst_" and "scnd_" tables in the same way as it is done for "events_all"

TODO: create unit tests for first and second session tables in these files. Create a separate code file that creates tables in right order, separate from the testing.



