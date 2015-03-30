

conn_str ="redshift+psycopg2://****"


# redshift doesn't support materialized views yet
def create_materialized_view(view_name, sql):
    view_intro = "DROP TABLE IF EXISTS $view_name CASCADE;\
    CREATE TABLE $view_name AS "
    return view_intro.replace('$view_name', view_name)+sql

def create_view(view_name, sql):
    # I made them all materialized to speed up queries
    # Have used of 100GB of the redshift cluster this way
    # TODO: convert some intermediary queries into views 
    # to save space when get next batch of data
    return create_materialized_view(view_name,sql)


def clean_string_for_column_name(s):
    s = s.replace(' ','_')
    s = s.replace('-','_')
    s = s.replace(':','')
    s = s.replace('$','')
    return s


# used in joins on id
def get_all_columns_without_id(table):

    engine = create_engine(conn_str)

    sql = """SELECT attrelid::regclass, attnum, attname
    FROM   pg_attribute
    WHERE  attrelid = '$table$'::regclass
    AND    attnum > 0
    AND    NOT attisdropped
    ORDER  BY attnum;"""

    sql = sql.replace("$table$", table)
    
    df = pd.read_sql(sql, engine)
    df = df[(df.attname != "distinct_id")]
    column_aliases= {}
    for col in df.attname.values:
        column_aliases[col] = col
    return column_aliases


