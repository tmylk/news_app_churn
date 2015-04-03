# Finding stories that make users come back

## Creating features from a raw Mixpanel import

1) Export from Mixpanel with mixpanel_import/import.py

2) import the csv into Redshift table "raw_copy"

3) To create features run these scripts in bash:

nosetests tests:core:tests_main_features
nosetests tests:first_session:test_time_features
nosetests tests:first_session:tests_story_features
nosetests tests:first_session:all_feature_user_tests.test_create_users_all_story_features
nosetests tests:second_session:test_event_features
nosetests tests:second_session:test_time_features

Note:
The tests_main_features file has meaningful tests for this use.
However tests passing/failing in other files mean nothing in the context of first_session and second_session events - they only test events_all.

I wrote these tests as I was writing SQL queries to extract features from the table containing all events "events_all". The order in which the tables need to be created is recorded in the order in which the tests are to be run. 

Later I needed to run the same SQL queries on subsets of the events - "first session" and "second session". A quick way was to inherit the "events_all" tests to get the same sequence of SQL queries.

The files in tests.first_session and tests.second_session will create the tables but the tests there won't be checking anything. The user_ids and table names in the asserts are for the main tables not the "frst_" or "scnd_" prefixed tables. However they will create "frst_" and "scnd_" tables in the same way as it is done for "events_all". I am re-using the code that I verified on events_all in the same order.

TODO: create separate unit tests for first and second session tables in these files. Create a separate code file that creates tables in right order, separate this logic from the testing asserts.



