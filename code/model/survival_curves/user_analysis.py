
from sqlalchemy import create_engine
import psycopg2
import nose.tools as n
import pandas as pd


conn_str = "redshift+psycopg2://***.redshift.amazonaws.com:5439/tmdb"



# drop columns that are always zero
# original 189
# for dirty data used to be 153
# for clean users 87! data is quite sparse
def drop_zero_cols(df_original):
    df = df_original.copy()
    df[df==0] = np.nan 
    df=df.dropna(axis=1,how='all',thresh=100)
    return df_original[df.columns]

def get_data():
    engine = create_engine(conn_str)
    df = pd.read_sql("SELECT * FROM clean_users",engine)
    return df

def down_sample(size, name, df):
    rows = np.random.choice(df.index.values, size)
    sampled_df = df.ix[rows]
    sampled_df.to_sql(name, engine, if_exists='replace')

def add_date_day(df, column):
    df[column + '_date'] =  pd.to_datetime(df[column], unit='s')
    df[column + '_day'] = df[column + '_date'].apply(lambda dt : datetime.date(dt.year, dt.month, dt.day))

    


def clean(df):
    df = drop_zero_cols(df) 
    add_date_day(df,'last_seen_ts')
    add_date_day(df,'first_seen_ts')      
    
    
    return df 

def transform_for_life_lines(df, cut_off_date):
    #remove those who started after cut_off
    df_ll =df[df.first_seen_ts_date < pd.to_datetime(cut_off_date)] 

    #populate the time of death as pd.NaT for thouse users who are before cut off 
    df_ll['last_seen_ll'] =df_ll.last_seen_ts_date.apply(lambda x : np.nan if x >=pd.to_datetime(cut_off_date) else x) 
    T, C = datetimes_to_durations(df_ll.first_seen_ts_date, df_ll.last_seen_ll, fill_date=pd.to_datetime(cut_off_date), freq='D')
    return (T,C)

  
#select power users
def get_power(df):
    power_condition_1 = (df.story_viewed_total > 0) & (df.age_days > 1) & (df_cut.time_to_100_agg_day_count>0)

    #or just this 
    power_condition_2 = (df_cut.time_to_100_agg_day_count>0)

    #two problems: survival past 0 day and survival long term

    
    
    cut_off_date ='2015-02-23'
    t = np.linspace(-1,40,42)
    first_days = 2

    kmf = KaplanMeierFitter()

    T,C =  transform_for_life_lines(df[ (df.time_to_100_agg_day_count==0) & (df.age_days > first_days)], cut_off_date)
    kmf.fit(T, C, label='didnt read to the end, size:'+str(len(T)), app=t)


    ax = kmf.plot()

    T_p,C_p =  transform_for_life_lines(df[(df.time_to_100_agg_day_count>0) & (df.age_days > first_days)], cut_off_date)
    kmf.fit(T_p, C_p, label='read_to_the_end_at_least_once, size:'+str(len(T_p)), app=t)
    kmf.plot(ax=ax)

    plt.title("Users who survived "+str(first_days) +" days")
    plt.xlabel('user age in days')

def compare_two_cohorts(df, cond_A, cond_B, cut_off_date):
    transform_for_life_lines(
    cut_off_date ='2015-02-23'
    t = np.linspace(-1,5,6)
    first_days = 1

    kmf = KaplanMeierFitter()

    T,C =  transform_for_life_lines(df[ (df.time_to_100_agg_day_count==0) & (df.age_days > first_days)], cut_off_date)
    kmf.fit(T, C, label='didnt read to the end', app=t)


    ax = kmf.plot()

    T_p,C_p =  transform_for_life_lines(df[(df.time_to_100_agg_day_count>0) & (df.age_days > first_days)], cut_off_date)
    kmf.fit(T_p, C_p, label='read_to_the_end_at_least_once', app=t)
    kmf.plot(ax=ax)


age_related_terms = ('_total', '_count', '_active', 'age_days', '_seen_ts', 'seen_ts_date', 'seen_ts_day')
def age_related_column(x):
        for w in age_related_terms:
            if x.endswith(w):
                return True
        return False

def create_stay_col(df, first_seen_cut_off_date, last_seen_cut_off_date):
    df_stay =df[df.first_seen_ts_date < pd.to_datetime(first_seen_cut_off_date)] 

    df_stay['stay'] = (df_stay.last_seen_ts_date >= pd.to_datetime(last_seen_cut_off_date))
    return df_stay
            

def get_non_age_related_columns(df):   

    return filter(lambda x: not age_related_column(x), df.columns)

def get_rf_data(df):
    data = df.copy()
    
    ids = ['distinct_id']
    categorical_cols = [u'city', u'mp_country_code']

    #remove age and categorical cols and ids
    cols = get_non_age_related_columns(df)
    cols = set(cols)
    cols.difference_update(set(categorical_cols))
    cols.difference_update(set(ids))
    
    data = data[list(cols)]
    y = data.pop('stay').values
    X = data.values.astype(float)
    return X, y, data


#random downsample only a certain class  
def down_sample(df, size, label):
    df_majority_class = df.stay == label
    df_minority_class = df.stay != label
    
    rows = np.random.choice(df.index.values, size)
    sampled_df_majority_class = df.ix[rows]
    return sampled_df_majority_class.append(df_minority_class)

def weighted_forest():
    ntrees = 50
    rf = RandomForestClassifier(oob_score=True, n_estimators = ntrees)
    w = y_train/y_train.mean() + 1
    rf.fit(X_train, y_train, sample_weight=w)
    get_output(rf, 'Weighted', data)

    print "OOB:", rf.oob_score_

    top_5_feats = np.argsort(rf.feature_importances_)[::-1][:5]
    print "Top 5 features:", data.columns[top_5_feats] 

# needs to run globally out of func to get output
#messes up the big X,y
def downsampled_forest(df_in, y):
    df = df_in.copy()
    df_downsampled = down_sample(df)
    X,y, data = get_rf_data(df_downsampled)
    X_train, X_test, y_train, y_test = train_test_split(X, y, 
        test_size=0.33, random_state=42)

    ntrees = 50
    rf = RandomForestClassifier(oob_score=True, n_estimators = ntrees)
    
    rf.fit(X_train, y_train)
    get_output(rf, 'Downsampled ', data)

    print "OOB:", rf.oob_score_

    top_5_feats = np.argsort(rf.feature_importances_)[::-1][:5]
    print "Top 5 features:", data.columns[top_5_feats] 

def get_output(rf, name, data):
    print "======"
    print name
    print "======"
    print 'Score on test data:', rf.score(X_test, y_test)

    y_pred = rf.predict(X_test)
    # row is true labels, column is predicted labels
    conf_mat = confusion_matrix(y_test, y_pred)  
    print 'matrix:' 
    print conf_mat    
    
    precision = float(conf_mat[1,1]) / (conf_mat[1,1] + conf_mat[0,1])
    recall =  float(conf_mat[1,1]) / (conf_mat[1,1] + conf_mat[1,0])
    print "Precision :", precision, "Recall:", recall
    
    pred_stay = sum(y_pred)
    act_stay = sum(y_test)
    pred_churn = len(y_pred) - sum(y_pred)
    act_churn = len(y_test) - sum(y_test)

    
    print "Actual churn :", act_churn, "Predicted churn:", pred_churn
    print "Actual stay :", act_stay, "Predicted stay:", pred_stay
    print "Percent of predicted staying out of all staying :", float(pred_stay)/act_stay 
    
    
   

if __name__ == '__main__':
    df = get_data()
    df = clean(df)
    first_seen_cut_off_date ='2015-02-16'
    last_seen_cut_off_date ='2015-02-23'
    df_churn = create_stay_col(df, cut_off_date)


#Feature importance
# ======
# Weighted
# ======
# Score on test data: 0.95757885288
# matrix:
# [[59970  1012]
#  [ 1729  1903]]
# Precision : 0.652830188679 Recall: 0.523953744493
# Actual churn : 60982 Predicted churn: 61699
# Actual stay : 3632 Predicted stay: 2915
# Percent of predicted staying out of all staying : 0.802588105727
# OOB: 0.957525955514
# Top 5 features: Index([u'app_viewed_day_avg', u'time_to_10_agg_day_avg', u'story_viewed_day_avg', u'app_navigation_clicked_day_avg', u'time_to_25_agg_day_avg'], dtype='object')
# ======
# Downsampled 
# ======
# Score on test data: 0.937065851625
# matrix:
# [[3255  340]
#  [ 113 3490]]
# Precision : 0.911227154047 Recall: 0.968637246739
# Actual churn : 3595 Predicted churn: 3368
# Actual stay : 3603 Predicted stay: 3830
# Percent of predicted staying out of all staying : 1.06300305301
# OOB: 0.938201478237
# Top 5 features: Index([u'time_to_10_agg_day_avg', u'app_viewed_day_avg', u'app_navigation_clicked_day_avg', u'story_viewed_day_avg', u'time_to_50_agg_day_avg'], dtype='object')

non_zero_columns = 
{u'age_days',
 u'city',
 u'curator_navigation_clicked_day_active',
 u'curator_navigation_clicked_day_avg',
 u'curator_navigation_clicked_total',
 u'curator_page_viewed_day_active',
 u'curator_page_viewed_day_avg',
 u'curator_page_viewed_total',
 u'curator_viewed_day_active',
 u'curator_viewed_day_avg',
 u'curator_viewed_total',
 u'distinct_id',
 u'explore_topic_clicked_day_active',
 u'explore_topic_clicked_day_avg',
 u'explore_topic_clicked_total',
 u'explore_viewed_day_active',
 u'explore_viewed_day_avg',
 u'explore_viewed_total',
 u'facebook_anxiety_alleviator_shown_day_active',
 u'facebook_anxiety_alleviator_shown_day_avg',
 u'facebook_anxiety_alleviator_shown_total',
 u'first_seen_ts',
 'first_seen_ts_date',
 'first_seen_ts_day',
 u'last_seen_ts',
 'last_seen_ts_date',
 'last_seen_ts_day',
 u'menu_clicked_day_active',
 u'menu_clicked_day_avg',
 u'menu_clicked_total',
 u'mp_country_code',
 u'notification_permission_accepted_day_active',
 u'notification_permission_accepted_day_avg',
 u'notification_permission_accepted_total',
 u'notification_permission_first_alert_shown_day_active',
 u'notification_permission_first_alert_shown_day_avg',
 u'notification_permission_first_alert_shown_total',
 u'notification_permission_first_request_accepted_day_active',
 u'notification_permission_first_request_accepted_day_avg',
 u'notification_permission_first_request_accepted_total',
 u'notification_permission_second_alert_shown_day_active',
 u'notification_permission_second_alert_shown_day_avg',
 u'notification_permission_second_alert_shown_total',
 u'share_story_clicked_day_active',
 u'share_story_clicked_day_avg',
 u'share_story_clicked_total',
 u'story_completion_day_active',
 u'story_completion_day_avg',
 u'story_completion_total',
 u'story_navigation_clicked_day_active',
 u'story_navigation_clicked_day_avg',
 u'story_navigation_clicked_total',
 u'story_viewed_day_active',
 u'story_viewed_day_avg',
 u'story_viewed_total',
 u'time_to_100_agg_day_avg',
 u'time_to_100_agg_day_count',
 u'time_to_100_agg_day_total',
 u'time_to_10_agg_day_avg',
 u'time_to_10_agg_day_count',
 u'time_to_10_agg_day_total',
 u'time_to_25_agg_day_avg',
 u'time_to_25_agg_day_count',
 u'time_to_25_agg_day_total',
 u'time_to_50_agg_day_avg',
 u'time_to_50_agg_day_count',
 u'time_to_50_agg_day_total',
 u'time_to_75_agg_day_avg',
 u'time_to_75_agg_day_count',
 u'time_to_75_agg_day_total',
 u'app_navigation_clicked_day_active',
 u'app_navigation_clicked_day_avg',
 u'app_navigation_clicked_total',
 u'app_scene_selected_day_active',
 u'app_scene_selected_day_avg',
 u'app_scene_selected_total',
 u'app_viewed_day_active',
 u'app_viewed_day_avg',
 u'app_viewed_total',
 u'topic_story_clicked_day_active',
 u'topic_story_clicked_day_avg',
 u'topic_story_clicked_total',
 u'topic_viewed_day_active',
 u'topic_viewed_day_avg',
 u'topic_viewed_total'}