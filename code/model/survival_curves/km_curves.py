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
            print "ax is none"
            ax = kmf.plot(c=color)
        else:
            kmf.plot(ax=ax, c=color)
            print "plotted second or third one"
        print "Painted", label_text
   
    
    plt.title("Users who survived >"+str(first_days) +" days")
    plt.xlabel('user age in days')
    
    # Shrink current axis by 20%
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])

    # Put a legend to the right of the current axis
    ax.legend(loc='lower left', bbox_to_anchor=(0, 1))
    
    return ax