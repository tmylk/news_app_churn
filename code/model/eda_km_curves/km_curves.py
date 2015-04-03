
from sqlalchemy import create_engine
import psycopg2
import nose.tools as n
import pandas as pd

###data prep

conn_str = "redshift+psycopg2://***.redshift.amazonaws.com:5439/tmdb"

def get_data():
    engine = create_engine(conn_str)
    df = pd.read_sql("SELECT * FROM clean_users",engine)
    return df




def add_date_day(df, column):
    df[column + '_date'] =  pd.to_datetime(df[column], unit='s')
    df[column + '_day'] = df[column + '_date'].apply(lambda dt : datetime.date(dt.year, dt.month, dt.day))

# drop columns that are always zero
# original 189
# for dirty data used to be 153
# for clean users 87
def drop_zero_cols(df_original):
    df = df_original.copy()
    df[df==0] = np.nan 
    df=df.dropna(axis=1,how='all',thresh=100)
    return df_original[df.columns]
   

def clean(df):
    df = drop_zero_cols(df) 
    add_date_day(df,'last_seen_ts')
    add_date_day(df,'first_seen_ts')
    return df 


age_related_terms = ('_total', '_count', '_active', 'age_days', '_seen_ts', 'seen_ts_date', 'seen_ts_day')
def age_related_column(x):
        for w in age_related_terms:
            if x.endswith(w):
                return True
        return False

# remove any columns that can leak the target. The target here is age
def get_non_age_related_columns(df):   
    return filter(lambda x: not age_related_column(x), df.columns)


def create_stay_col(df, first_seen_cut_off_date, last_seen_cut_off_date):
    df_stay =df[df.first_seen_ts_date < pd.to_datetime(first_seen_cut_off_date)] 

    df_stay['stay'] = (df_stay.last_seen_ts_date >= pd.to_datetime(last_seen_cut_off_date))
    return df_stay

#########

# once it is cleaned, can save and re-use from csv
def get_data_from_csv():
    df = pd.read_csv('all_users_stay.csv', encoding='utf-8') 
    df.first_seen_ts_date = pd.to_datetime(df.first_seen_ts_date)
    df.last_seen_ts_date = pd.to_datetime(df.last_seen_ts_date)
    return df            

##########


def get_regr_data(df, cut_off_date):
    data = df.copy()
    #remove those who started after cut_off
    data['ll_duration']  =df.age_days
    data['ll_event'] = (data.last_seen_ts_date < pd.to_datetime(cut_off_date))
    
    data =data[data.first_seen_ts_date < pd.to_datetime(cut_off_date)] 
    T = data['ll_duration']
    C = data['ll_event']
    print data.shape

      
    ids = ['distinct_id']
    categorical_cols = [u'city', u'mp_country_code']

    #remove age and categorical cols and ids
    cols = get_non_age_related_columns(df)
    cols = set(cols)
    cols.difference_update(set(categorical_cols))
    cols.difference_update(set(ids))
    
    data = data[list(cols)]
    #scaling
    data = pd.DataFrame(preprocessing.scale(data), index=data.index, columns=data.columns)
    
    data.drop(u'Unnamed: 0', axis=1, inplace=True)
  
        
    data['ll_duration'] = T
    data['ll_event'] = C 
    
    
    return data




def plot_KM(df, conditions, label_texts, colors=['b'],first_days = 0, cut_off_date=pd.to_datetime('2015-02-28'), first_seen_date=pd.to_datetime('2015-01-15') ,     ax = None):

    print "AAA"
    data = get_regr_data(df, cut_off_date)
    days_difference = (cut_off_date - first_seen_date).days
    t = np.linspace(-1,days_difference,days_difference +1)

    for (condition,label_text, color) in zip(conditions, label_texts, colors):
        kmf = KaplanMeierFitter()

        selected = data[(condition) & (df.age_days > first_days) & (df.first_seen_ts_date > first_seen_date )]
        kmf.fit(selected['ll_duration'], selected['ll_event'], label=label_text + ', size:'+str(len(selected)), app=t)

        if not ax:
            ax = kmf.plot(c=color)
        else:
            kmf.plot(ax=ax, c=color)
   
    
    plt.title("Users who survived >"+str(first_days) +" days")
    plt.xlabel('user age in days')
    
    # Shrink current axis by 20%
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])

    # Put a legend to the right of the current axis
    ax.legend(loc='lower left', bbox_to_anchor=(0, 1))
    
    return ax

if __name__ == '__main__':
# on first run fetch data from db, clean it and save as csv
    # df = get_data()
    # df = clean(df)
    # first_seen_cut_off_date ='2015-02-16'
    # last_seen_cut_off_date ='2015-02-23'
    # df_stay = create_stay_col(df, cut_off_date)

# then just use the csv local on the vm
# it allows to shutdown the redshift cluster to save funds
    df = get_data_from_csv()

    # many curves were plotted, these two are the most important ones
    # the interesting part here is that they are almost the same, see pdf presentation
    conditions = [  (df.time_to_100_agg_day_count>0) & 
                    (df.time_to_10_agg_day_count>0) &
                    (df.story_viewed_day_avg>0),
                    (df.time_to_100_agg_day_count==0) &
                    (df.time_to_10_agg_day_count>0)  &
                    (df.story_viewed_day_avg>0)]
    labels = ["read to 100 at least once", "read to 10 but not 100"]
    colors = ['g', 'b']
    ax =  plot_KM(df, conditions, labels, ax=ax, first_days=2, cut_off_date ='2015-03-01')
