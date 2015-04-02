
# Creating features for general churn analysis from a raw Mixpanel import

1) Export from Mixpanel with mixpanel_import/import.py

2) import the csv into Redshift table "raw_copy"

3) Create features
Run in bash:

nosetests tests:core:tests_main_features
nosetests tests:events_all

If the data is for the same period as my analysis then all the tests should pass. (I wrote these tests as I was writing SQL queries to extract features from the table containing all events "events_all".)

TODO: create a test db with mock data to run tests on. Extract the order of creating tables from tests into a file in code folder.