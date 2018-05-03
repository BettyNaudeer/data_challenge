
# coding: utf-8

# ### Goal
# We would like you to demonstrate your skills in simple data manipulation and analysis. We are looking to understand how you approach the problem and knowledge of programming / data analysis skills, attention to detail and ability to learn new things on the fly.
#
# ### Data
# We have provided you with a dataset of app reviews for the popular reddit app. The CSV file has multiple flaws and falls short of what we would like to work with, as is often the case and therefore we want you to fix it.
#
# ### Expected Output
# We expect you to parse the file and make sure that things are consistent and as orderly as possible. For example the app_bought and money_spent variables should be available in buckets (feel free to choose the buckets yourself). We will leave it to you to define what is desirable.
#
# You should produce these outputs in a ZIP archive or as Pull Request to this repo:
#
# A new CSV file with clean data (see above) We would want you to populate the reviews table in exercise_database.db (a SQLlite database). It holds the following columns: review title iso score date apps_bought money_spent apps_bought_bucket money_spent_bucket TEXT TEXG TEXT INTEGER TEXT INTEGER NUMERIC TEXT TEXT Add the updated exercise_database.db and the code used to populate the database to the archive.
#
# Once you have the table ready, please write SQL queries to fetch the following metrics from the reviews table. Average score by iso Maximum score by app_bought_bucket Average score over time (day) Add the code used to generate the queries and a CSV with results into the archive.
#


import pandas as pd
import numpy as np
import datetime, time
import sqlite3
from pandas import DataFrame

#Write a short mapper that bins data
def map_bin(x, bins):
    kwargs = {}
    if x == max(bins):
        kwargs['right'] = True
    bin = bins[np.digitize([x], bins, **kwargs)[0]]
    bin_lower = bins[np.digitize([x], bins, **kwargs)[0]-1]
    return '[{0}-{1}]'.format(bin_lower, bin)

def clean_up_df(df):
    #declare bins
    app_bought_bins = np.array([0, 20, 40, 60, 80, 100])
    money_spent_bins = np.array([0, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500])

    #make the bins for apps bought and money spent
    df['apps_bought_bucket'] = df['app_bought'].apply(map_bin, bins=app_bought_bins)
    df['money_spent_bucket'] = df['money_spent'].apply(map_bin, bins=money_spent_bins)

    #aligning names with database
    df=df.rename(columns = {"app_bought": "apps_bought"})
    df = df.drop('product_name', 1)

    #formatting dates
    cleaned_dates = clean_dates(df)
    df['dates'] = cleaned_dates

    return df


def clean_dates(df):
    cleaned_dates = []
    year_without_padding = '%y-%m-%d %H:%M:%S'
    day_month_year = '%d/%m/%y'
    year_with_padding = '%Y-%m-%d %H:%M:%S'

    for date in df['date']:
        if (convert_date(date, year_without_padding)):
            cleaned_dates.append(convert_date(date, year_without_padding))
        elif (convert_date(date, day_month_year)):
            cleaned_dates.append(convert_date(date, day_month_year))
        elif (convert_date(date, year_with_padding)):
            cleaned_dates.append(convert_date(date, year_with_padding))
        else:
            print("fail")

    return cleaned_dates

#function allowing multiple date formats
def convert_date(date, date_format):
    try:
        return datetime.datetime.strptime(date, date_format)
    except Exception as e:
        return None

#connect to db
def make_db(df):
    conn = sqlite3.connect("./db/exercise_database.db")
    cur = conn.cursor()
    df.to_sql(name="reviews", con=conn, if_exists = 'replace', index=False)

#connect to db with queries
def query_db(sql_query):
    conn = sqlite3.connect("./db/exercise_database.db")
    cur = conn.cursor()
    cur.execute(sql_query)
    sql_query_df = DataFrame(cur.fetchall())
    return(sql_query_df)

#make csv with average_score_iso
def average_score_iso():
    average_score_iso_sql = "SELECT iso, AVG(score) AS AvgIsoScore FROM reviews GROUP BY iso;"
    average_score_iso_df = query_db(average_score_iso_sql)

    average_score_iso_df.to_csv("./metrics/Average score by `iso`", sep='\t')

#make csv with max_score_by_apps_bought
def max_score_by_apps_bought():
    max_score_sql = "SELECT apps_bought_bucket, MAX(score) AS MaxScore FROM reviews GROUP BY apps_bought_bucket;"
    max_score_df = query_db(max_score_sql)

    max_score_df.to_csv("./metrics/Maximum score by `app_bought_bucket`", sep='\t')

#make csv with average_score_over_day
def average_score_over_day():
    average_score_over_day_sql = "SELECT date, AVG(score) AS AvgDateScore FROM reviews GROUP BY date;"
    average_score_over_day_df = query_db(average_score_over_day_sql)

    average_score_over_day_df.to_csv("./metrics/Average score over time (day)", sep='\t')



def main():
    df = pd.read_csv("./resources/reddit_exercise_data.csv")
    df = df.loc[:, (df != 0).any(axis=0)]
    cleaned_df = clean_up_df(df)
    make_db(cleaned_df)
    average_score_iso()
    max_score_by_apps_bought()
    average_score_over_day()

main()
