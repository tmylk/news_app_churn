# Creating features for the model

In order to clean data, do EDA and for finding events that make users stay longer with the app the following features need to be created.

For stories_that_retain analysis see README in that foler.

## Creating features from a raw Mixpanel import

1) Export from Mixpanel with mixpanel_import/import.py

2) import the csv into Redshift table "raw_copy"

3) Create features
Run in bash:

nosetests tests:core:tests_main_features
nosetests tests:events_all:test_event_features
nosetests tests:events_all:test_time_features
nosetests tests:events_all:all_features_user_tests

If the data is for the same period as my analysis then all the tests should pass. (I wrote these tests as I was writing SQL queries to extract features from the table containing all events "events_all".)

TODO: create a test db with mock data to run tests on so tests are independent from actual data. Extract the order of creating the tables from tests into a separate file and put it into the code folder.