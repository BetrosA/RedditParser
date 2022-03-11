import datetime
import requests
import pandas as pd
from pprint import pprint
from time import sleep

url = 'https://api.pushshift.io/reddit/search/submission/'

def get_pushshift_data(after, before, sub, keyword):
  # ?&after=' + \
  #     str(after)+'&before='+str(before) + \
  #     '&subreddit='+str(sub)+'&q='+str(keyword)

  print(url)
  request = requests.get(url, params={"after": after, "before": before, "subreddit": sub, "q": keyword, "size": 500})
  return request.json()


beginYear = 2020 #input("What year do you want to begin searching reddit from?: ")
beginMonth = 1 #input("What month of that year? (In number - 12): ")
beginDay = 30# input("And day?: ")
endYear = 2022 #input("Now for the end year of your time search?: ")
endMonth = 1 #input("Ending month?: ")
endDay = 30 #input("Finally, the last day of your searching time?: ")

beginYear = int(beginYear)
beginMonth = int(beginMonth)
beginDay = int(beginDay)
endYear = int(endYear)
endMonth = int(endMonth)
endDay = int(endDay)

timeStart = datetime.datetime(
    beginYear, beginMonth, beginDay, 0, 0).timestamp()
timeEnd = datetime.datetime(endYear, endMonth, endDay, 0, 0).timestamp()

timeStart = int(timeStart)
timeEnd = int(timeEnd)

sub = "uberdrivers" #input("What subreddit would you like to seach in?: ")
keyword = "depressed|depression|depressing" #input("What keyword are you searching for?: ")

posts = get_pushshift_data(timeStart, timeEnd, sub, keyword)["data"]

entries = []

for post in posts:
  # post["body"] = post["selftext"]
  # del post["selftext"]
  entries.append(post)
  
  request_start = datetime.datetime.now()
  try:
    request = requests.get('https://api.pushshift.io/reddit/search/comment/',
                          params={"link_id": post["id"], "sort": "score:desc", "size": 500})
    replies = request.json()["data"]
  except Exception as e:
    print(f"Request exception: {e}")
    replies = []
    
  elapsed = (datetime.datetime.now() - request_start).total_seconds()
  if elapsed < 1:
        sleep(1.01 - elapsed)
  

  entries.extend(replies)

for entry in entries:
      entry["permalink"] = "https://www.reddit.com"+entry["permalink"]

desired_columns = ["id","author", "title", "subreddit","num_comments", "score", "selftext", "body", "permalink"]

whole_df = pd.DataFrame.from_records(entries)
for column_name in desired_columns:
      if column_name not in whole_df:
            whole_df[column_name] = ""

specific_columns_df = whole_df[desired_columns]

specific_columns_df.to_csv("reddit_data.csv")
print(specific_columns_df)