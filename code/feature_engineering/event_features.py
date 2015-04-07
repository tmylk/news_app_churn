from sqlalchemy import create_engine
import pandas as pd
from helpers import conn_str
import features
import helpers

# features needed for event stats analysis in a subset of events


class event_features(object):

    levels = [u'100', u'75', u'10', u'50', u'25']

    def __init__(self, event_table_name, event_table_alias):
        '''
        creates separate stats tables for different subset of events
        for example if called with
        event_table_name = first_session_events, event_table_alias = "frst_£
        the this class will create all tables with prefix "frst_"
        '''
        self.event_table_name = event_table_name
        self.event_table_alias = event_table_alias

    def create_view(self, view_name, sql):
        return helpers.create_materialized_view(
            self.event_table_alias + view_name, sql)

    def create_day_totals(self):
        view_name = "day_totals"
        return self.create_view(view_name,
                                """WITH d AS (
            SELECT * FROM dates_during_users_life
            CROSS JOIN event_types
            )
        SELECT
            d.date AS date,
            d.week AS week,
            d.distinct_id,
            d.event_type,
            COALESCE(COUNT(r.time),0) AS day_total
        FROM """ + self.event_table_name + """ r
        RIGHT JOIN d ON
            DATE(r.time) = d.date AND
            r.distinct_id = d.distinct_id AND
            r.event_type = d.event_type
         GROUP BY d.week, d.date, d.distinct_id, d.event_type""")

    # TODO: maybe exclude weeks 3 and 9 from analysis
    #      as they are shorter than normal?
    # TODO: maybe use rolling average instead as in first week of users life?
    def create_week_totals(self):
        view_name = "week_totals"
        return self.create_view(view_name, '''
            SELECT
                SUM(day_total) AS week_total,
                distinct_id,
                week,
                event_type
            FROM ''' + self.event_table_alias + '''day_totals
            GROUP BY week, distinct_id, event_type''')

    def create_all_week_event_stats(self):
        queries = []
        engine = create_engine(conn_str)
        df_events = pd.read_sql("SELECT * FROM event_types", engine)
        for event_type in df_events.event_type.values:
            sql_event_type = helper.clean_string_for_column_name(event_type)
            view_name = sql_event_type + "_week_stats"
            sql = """SELECT distinct_id,
                        AVG(week_total::real) as week_avg,
                        SUM(week_total) as total,
                        SUM(CASE WHEN week_total = 0 THEN 0 ELSE 1 END)
                         as week_active
                     FROM """ + self.event_table_alias + """week_totals
                     WHERE event_type='$event_type$'
                     GROUP BY distinct_id;
                     """
            sql = sql.replace("$event_type$", event_type)
            queries.append(self.create_view(view_name, sql))

        return ''.join(queries)

    def create_all_day_event_stats(self):
        queries = []
        engine = create_engine(conn_str)
        df_events = pd.read_sql("SELECT * FROM event_types", engine)
        for event_type in df_events.event_type.values:
            sql_event_type = helpers.clean_string_for_column_name(event_type)
            view_name = sql_event_type + "_day_stats"
            sql = """SELECT distinct_id,
                            AVG(day_total::real) as day_avg,
                            SUM(day_total) as total,
                            SUM(CASE WHEN day_total = 0 THEN 0 ELSE 1 END)
                                     as day_active
                     FROM """ + self.event_table_alias + """day_totals
                     WHERE event_type='$event_type$'
                     GROUP BY distinct_id;
                     """
            sql = sql.replace("$event_type$", event_type)
            queries.append(self.create_view(view_name, sql))

        return ''.join(queries)

    def create_all_agg_day_event_stats(self):
        queries = []
        engine = create_engine(conn_str)
        df_events = pd.read_sql("SELECT * FROM event_types", engine)
        for event_type in df_events.event_type.values:
            sql_event_type = helpers.clean_string_for_column_name(event_type)
            view_name = sql_event_type + "_agg_day_stats"
            sql = """SELECT d.week,
                            d.date,
                            AVG(day_total::real) as agg_day_avg,
                            SUM(day_total::real) as agg_day_total,
                            SUM(CASE WHEN day_total = 0 THEN 0 ELSE 1 END)
                                 as agg_day_unique
                     FROM """ + self.event_table_alias + """day_totals d
                     WHERE event_type='$event_type$'
                     GROUP BY d.week, d.date;
                     """
            sql = sql.replace("$event_type$", event_type)
            queries.append(self.create_view(view_name, sql))

        return ''.join(queries)

    def create_time_to_x_day_totals(self):
        '''
        how often a user read X% of the article
        and how long it took them to get there
        '''
        view_name = "time_to_x_day_totals"
        return self.create_view(view_name, """
        WITH d AS (
            SELECT * FROM dates_during_users_life
            CROSS JOIN event_types
            )
        SELECT
            d.date AS date,
            d.week AS week,
            d.distinct_id,
            d.event_type,
            COALESCE(r."story completion",'0') AS "story completion",
            r."story id",
            SUM(COALESCE("time since story opened",'0')) AS day_time_total,
            COALESCE(COUNT("time since story opened"),'0') AS day_count,
            AVG(COALESCE("time since story opened",'0')::real) AS day_time_avg
        FROM """ + self.event_table_name + """ r
        RIGHT JOIN d ON
            DATE(r.time) = d.date AND
            r.distinct_id = d.distinct_id AND
            r.event_type = d.event_type AND
            r.event_type = 'story completion'
         GROUP BY d.week, d.date, d.distinct_id,
                     d.event_type, r."story completion", r."story id" ;  """)

    def create_time_to_x_day_stats(self):
        view_name = "time_to_$level$_day_stats"

        sql = """
        SELECT distinct_id,
          "story completion",
          "story id",
           SUM(day_time_total) as agg_day_time_total,
           AVG(day_time_avg::real) as agg_day_time_avg,
           SUM(day_count) as agg_day_count,
           AVG(day_count::real) as agg_day_count_avg
        FROM """ + self.event_table_alias + """time_to_x_day_totals
        WHERE "story completion"=$level$
        GROUP BY distinct_id ,"story completion", "story id";
        """

        queries = []

        for level in self.levels:
            my_view_name = view_name.replace('$level$', level)
            my_sql = sql.replace("$level$", level)
            queries.append(self.create_view(my_view_name, my_sql))

        return ''.join(queries)

    def create_all_day_story_stats(self):
        queries = []
        engine = create_engine(conn_str)
        df_stories = pd.read_sql("SELECT * FROM stories", engine)

        level_view_name = "time_to_$level$_day_stats"
        # can't have 1000 features = 200 stories * 5 levels
        level = "100"
        my_level_view_name = level_view_name.replace('$level$', level)
        for story_id in df_stories["story id"].values:
            sql_story_id = helpers.clean_string_for_column_name(story_id)
            view_name = "story_" + sql_story_id + "_day_stats"
            sql = """
                    SELECT f.distinct_id,
                    COALESCE(agg_day_time_total,0) AS agg_day_time_total,
                    COALESCE(agg_day_time_avg,0) AS agg_day_time_avg,
                    COALESCE(agg_day_count,0) AS agg_day_count,
                    COALESCE(agg_day_count_avg,0) AS agg_day_count_avg
                    FROM
                     (SELECT * FROM """ + self.event_table_alias + my_level_view_name + """
                    WHERE "story id"='$story_id$' ) t
                    RIGHT JOIN first_seen_from_events f
                    ON f.distinct_id = """ + "t" + """.distinct_id;
                    """
            sql = sql.replace("$story_id$", story_id)
            queries.append(self.create_view(view_name, sql))

        return ''.join(queries)

    # NOTE: agg_day_count is how many times it hit the 100% in total.
    # not the number of unique days!
    def create_time_to_x_agg_day_stats(self):
        view_name = "time_to_$level$_agg_day_stats"

        sql = """
        SELECT f.distinct_id,
          "story completion",
           COALESCE(SUM(day_time_total),0) as agg_day_time_total,
           COALESCE(AVG(day_time_avg),0) as agg_day_time_avg,
           COALESCE(SUM(day_count),0) as agg_day_count,
           COALESCE(AVG(day_count),0) as agg_day_count_avg
        FROM
        (   SELECT *
            FROM """ + self.event_table_alias + """time_to_x_day_totals
            WHERE "story completion"=$level$
        ) t
        RIGHT JOIN first_seen_from_events f
        ON f.distinct_id = t.distinct_id
        GROUP BY f.distinct_id ,"story completion";
        """

        queries = []

        for level in self.levels:
            my_view_name = view_name.replace('$level$', level)
            my_sql = sql.replace("$level$", level)
            queries.append(self.create_view(my_view_name, my_sql))

        return ''.join(queries)

# Collecting features together
    def get_all_day_event_stats_tables(self, first, last):
        tables = {}

        engine = create_engine(conn_str)
        df_events = pd.read_sql(
            "SELECT * FROM event_types ORDER BY event_type ",
            engine)
        df_events = df_events[first:last]
        for event_type in df_events.event_type.values:
            column_aliases = {}
            sql_event_type = helpers.clean_string_for_column_name(event_type)
            view_name = self.event_table_alias + sql_event_type + "_day_stats"
            original_columns = ["day_avg", "total", "day_active"]
            for col in original_columns:
                column_aliases[col] = sql_event_type + "_" + col
            tables[view_name] = column_aliases
        return tables

    def get_all_story_stats_tables(self, first, last):
        tables = {}

        engine = create_engine(conn_str)

        df_stories = pd.read_sql(
            """SELECT * FROM stories ORDER BY "story id" """,
            engine)
        df_stories = df_stories[first:last]

        for story_id in df_stories["story id"].values:
            column_aliases = {}
            sql_story_id = helpers.clean_string_for_column_name(story_id)
            view_name = self.event_table_alias + \
                "story_" + sql_story_id + "_day_stats"
            # ,"agg_day_time_avg", "agg_day_count", "agg_day_count_avg"]
            original_columns = ["agg_day_count_avg"]
            for col in original_columns:
                column_aliases[col] = "story_" + sql_story_id + "_" + col
            tables[view_name] = column_aliases
        return tables

    def get_all_time_stats_tables(self):
        tables = {}
        view_name = self.event_table_alias + "time_to_$level$_agg_day_stats"
        for level in self.levels:
            column_aliases = {}
            my_view_name = view_name.replace('$level$', level)
            original_columns = [
                "agg_day_time_total",
                "agg_day_time_avg",
                "agg_day_count",
                "agg_day_count_avg"]
            for col in original_columns:
                column_aliases[col] = "time_to_" + level + "_" + col
            tables[my_view_name] = column_aliases

        return tables

    def get_all_static_features(self):
        tables = {}
        tables["last_seen_from_events"] = {"last_seen_ts": "last_seen_ts"}
        tables["first_seen_from_events"] = {"first_seen_ts": "first_seen_ts"}
        tables["age_from_events"] = {"age_days": "age_days"}
        tables["user_one_city_country"] = {
            "\"$city\"": "\"city\"",
            "mp_country_code": "mp_country_code"}
        tables["user_screen"] = {
            "max_width": "max_width",
            "max_height": "max_height"}

        return tables

    def get_all_first_last_event_tables(self):
        tables = {}
        tables["first_events"] = {
            "first_event_type": "first_event_type",
            "first_event_new_user": "first_event_new_user",
            "first_event_result": "first_event_result"}
        tables["last_events"] = {
            "last_event_type": "last_event_type",
            "last_event_new_user": "last_event_new_user",
            "last_event_result": "last_event_result"}
        return tables

    def aggregate_all_features(self, tables, use_star=False):
        start = """ SELECT """
        if use_star:
            new_tables = {}
            for table in tables:
                new_tables[table] = helpers.get_all_columns_without_id(table)
            tables = new_tables

        middle = tables.keys()[0] + ".distinct_id, "
        for tbl, col_aliases in tables.iteritems():
            for col, alias in col_aliases.iteritems():
                middle += tbl + "." + col + " AS " + alias + ", "
        middle = middle[:-2]

        middle += " FROM \""
        end = ""
        for tbl in tables.keys()[1:]:
            end += "\"" + \
                tables.keys()[0] + "\"" + ".distinct_id = \"" + \
                tbl + "\".distinct_id AND "
        end = end[:-4]

        sql = start + middle + "\", \"".join(tables) + "\" WHERE " + end
        return sql

    def create_users_all_static_features(self):
        view_name = "users_all_static_features"
        tables = self.get_all_static_features()
        tables["age_from_events"] = {"age_days": "age_days"}
        return self.create_view(view_name, self.aggregate_all_features(tables))

    def create_users_all_time_features(self):
        view_name = "users_all_time_features"
        tables = self.get_all_time_stats_tables()
        return self.create_view(view_name, self.aggregate_all_features(tables))

    # had to split into 2 chunks as the query was too
    #           long for redshift to process
    # TODO: do the same thing in Python in dictVectorizer and pickle .
    #       Feature activation is low, so the data is sparse.
    #       (Didn't expect it when started the project)
    #       Most probably don't need that much RAM to store sparse matrix
    #           so dictVectorizer should work fine.
    def create_users_all_event_features(self):
        view_name = "users_all_event_features_1"
        tables = self.get_all_day_event_stats_tables(0, 28)
        return self.create_view(view_name, self.aggregate_all_features(tables))

    def create_users_all_event_features_2(self):
        view_name = "users_all_event_features_2"
        tables = self.get_all_day_event_stats_tables(28, 56)
        return self.create_view(view_name, self.aggregate_all_features(tables))

    # TODO: try the same thing in Python in dictVectorizer and pickle.
    # See above
    # Performance was not a consideration for this 2 weeks project but need
    # to think about it in the future
    def create_users_all_story_features(self, i, first, last):
        view_name = "users_all_story_features_" + str(i)
        tables = self.get_all_story_stats_tables(first, last)
        return self.create_view(view_name, self.aggregate_all_features(tables))

    def create_users_all_features(self):
        view_name = "users_all_features"
        tables = {
            "users_all_static_features": "",
            self.event_table_alias +
            "users_all_time_features": "",
            self.event_table_alias +
            "users_all_event_features_1": "",
            "users_all_event_features_2": "",
        }
        return self.create_view(
            view_name, self.aggregate_all_features(tables, use_star=True))

    def create_users_all_stories(self):
        view_name = "users_all_stories"
        tables = {
            "users_all_static_features": ""}
        for i in xrange(7):
            tables[self.event_table_alias + "users_all_story_features_" + str(i) = ""
        return self.create_view(
            view_name, self.aggregate_all_features(tables, use_star=True))
