# news_app_churn

This is code for my 2 week capstone project at Zipfian Academy.

I analysed customer churn in a mobile news app.

[![Presentation](https://github.com/tmylk/news_app_churn/blob/master/news_app_churn.png)](https://github.com/tmylk/news_app_churn/blob/master/news_app_churn.pdf)

## Insights

I found that the company's user engagement metric didn't correspond to retention. I suggested a new metric and implemented it with the developers. See code/model/events_that_retain and code/model/eda_km_curves

I also found which news stories make first time customers come back. Moving from first to second visit was the biggest leak in the funnel at 50%. See code/model/stories_that_retain

## Modelling

For exploratory data analysis I drew lots of Kaplan-Meier survival curves. To find events that retain I used random forest's feature importance. It gave the insight about problems with the old user engagement metric. 

I used Bayesian hypothesis testing to find news stories with highest retention.

## Programming

The dataset was 8m events from Mixpanel json API imported into a Redshift SQL server. See code/mixpanel_import

I wrote unittests for the SQL feature engineering as I was writing queries following TDD. See tests/

Data analysis using python sklearn and lifelines package.

## Data cleaning

Some user sessions were broken into two or three pieces because of Mixpanel setup. See [my blog post](http://lev.ghost.io/2015/04/05/how-to-setup-mixpanel-for-churn-analysis/) on how to avoid it and get consistent user tracking. (Most interesting part is that there is no way to export aliasing events out of MP.)
