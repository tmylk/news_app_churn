import pandas as pd
from sqlalchemy import create_engine

# See README.md on how to create features for this analysis

conn_str = "redshift+psycopg2://levkonst:****.redshift.amazonaws.com:5439/**db"


def get_data():
    engine = create_engine(conn_str)
    df = pd.read_sql("""
    SELECT u.* FROM frst_users_all_stories u
    JOIN clean_users c
    ON c.distinct_id = u.distinct_id""", engine)
    return df


def get_title(story_id):
    i = story_id[6:-18]
    i = i.replace('_', '-')
    return df_stories[df_stories['story id'] == i].head()['max']

if __name__ == '__main__':
    df_stories = pd.read_sql("""SELECT * FROM stories""", engine)
    df = get_data()
    df_target = pd.read_sql("""SELECT u.* FROM scnd_time_to_100_agg_day_stats u
        JOIN clean_users c
        ON c.distinct_id = u.distinct_id""", engine)

    df_target = df_target.sort(columns='distinct_id')
    df = df.sort(columns='distinct_id')

    # create positive, negative and total counts
    p = {}
    n = {}
    t = {}
    all_reads = []
    ctr = Counter()
    for col in df_dropped.columns:
        total = ((df_dropped[col] > 0)).sum()
        all_reads.append(total)
        if total > 0:
            p[col] = ((df_dropped[col] > 0) &
                      (df_target.agg_day_count > 0)).sum()
            n[col] = ((df_dropped[col] > 0) &
                      (df_target.agg_day_count <= 0)).sum()
            t[col] = ((df_dropped[col] > 0)).sum()
            ctr[col] = p[col] / t[col]

    num_samples = 10000

    # H_0 = {every story is the same as this most popular story}
    # I don't need to use a prior to calc probability
    #                                   for this most popular story
    # because the sample size is large
    most_popular_story = \
        'story_d0ce59fe_1556_4448_8041_56bcfmdca402_agg_day_count_avg'
    old = np.random.beta(1 + p[most_popular_story], 1 + n[most_popular_story],
                         size=num_samples)

    # need this to guide the choice of a prior
    come_back_on_avg = (df_target.agg_day_count > 0).sum() / len(df_target)
    finished_on_both_visits = ((df_dropped.sum(axis=1) > 0) &
                               (df_target.agg_day_count > 0)).sum()

    # other stories have much less hits than the popular story so need a prior
    # 50 is the median total hits for all stories
    # so choose it as the # of observations for the prior
    # 10% is the mean positive outcome percentage, so use it in prior
    prior_p = 5
    prior_n = 45

    # posterior distrs
    distr = {}  # pos, neg, total
    for s in ctr.keys():
        distr[s] = np.random.beta(1 + p[s] + prior_p, 1 + n[s] + prior_n,
                                  size=num_samples)

    threshold = 0.8
    lift = 0.2

    better_than_old = {}
    truly_great = []

    for s1 in distr.keys():
        if not s1 == most_popular_story:
            better_than_old[s1] = (distr[s1] - old > lift * old).sum() \
                / num_samples
            if better_than_old[s1] > threshold:
                print "Success!", s1, get_title(s1), \
                    "is better than old story at ", better_than_old[s1], \
                    " prob with ctr=", ctr[s1]
                truly_great.append(s1)
                print p[s1], n[s1], t[s1]
