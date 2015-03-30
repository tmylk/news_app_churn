import pandas as pd
from sqlalchemy import create_engine

#TODO: reimport from pynb!!!!

# See readme.md on how to create features for this analysis

conn_str = "redshift+psycopg2://levkonst:****.redshift.amazonaws.com:5439/**db"

def get_data():
    engine = create_engine(conn_str)
    df = pd.read_sql("""SELECT u.* FROM frst_users_all_stories u
    JOIN clean_users c
    ON c.distinct_id = u.distinct_id""",engine)
    return df

def get_title(story_id):
    i = story_id[6:-18]
    i = i.replace('_','-')    
    return df_stories[df_stories['story id']==i].head()['max']


df_stories = pd.read_sql("""SELECT * FROM stories""", engine)
df = get_data() 
df_target = pd.read_sql("""SELECT u.* FROM scnd_time_to_100_agg_day_stats u
    JOIN clean_users c
    ON c.distinct_id = u.distinct_id""",engine)
    

df_target = df_target.sort(columns='distinct_id')
df = df.sort(columns='distinct_id')

# to create a prior
come_back_on_avg = (df_target.agg_day_count > 0).sum()/len(df_target) 
finished_on_both_visits = ((df_dropped.sum(axis=1) > 0)  &  (df_target.agg_day_count > 0 ) ).sum()



#
p = {}
n = {}
t = {}#pos, neg, total
all_reads=[]
ctr = Counter()
for col in df_dropped.columns:
 #story_922331f3_f8cd_40db_b234_fd773e002930_agg_day_count_avg
    total = ( (df_dropped[col] >0 ) ).sum()
    all_reads.append(total)
    if total > 0:
        p[col]= ((df_dropped[col] > 0 ) & (df_target.agg_day_count > 0) ).sum()
        n[col]= ( (df_dropped[col] >0 ) & (df_target.agg_day_count<=0) ).sum()
        t[col]= ( (df_dropped[col] >0 ) ).sum()   
        ctr[col] = p[col]/t[col]


pop_story = 'story_d0ce59ef_15a6_4448_8041_56bcfedca402_agg_day_count_avg'

# Assume 10%, the most popular story has this, but 45 is the median total
# so choose 50 as total here
prior_p = 5
prior_n = 45

num_samples = 10000


distr = {}#pos, neg, total
for s in ctr.keys():
    distr[s] = np.random.beta(1 + p[s] + prior_p,1 + n[s]+prior_n,           size=num_sampl

threshold = 0.8

old = np.random.beta(1 + p[pop_story],1 + n[pop_story],           size=num_samples)


can_comp = {}

truly_great = []
lift = 0.2
for s1 in distr.keys():    
        if  not s1==pop_story:
            can_comp[s1] = (distr[s1] - old > lift * old).sum()/num_samples
            if can_comp[s1] > threshold:
                print "Success!", s1, get_title(s1), "is better than old story at ", can_comp[s1], " prob with ctr=", ctr[s1]
                truly_great.append(s1)
                print p[s1], n[s1],t[s1]
                
       
                
print len(set(truly_great))