import datetime
import string
import requests
import pandas as pd
from pprint import pprint
from time import sleep

url = 'https://api.pushshift.io/reddit/search/submission/'

def get_pushshift_data(after, before, sub, keyword):

  print(url)
  
  request = requests.get(url, params={"after": after, "before": before, "subreddit": sub, "q": keyword, "size": 1})
  ##print(request)
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
keyword = "depressed|depression|depressing|" #input("What keyword are you searching for?: ")

# posts is a list of dictionaries, where each dictionary represents one post
# this contains all the data we got from the reddit API about the post with out comments
# posts are called submissions on the API
posts = get_pushshift_data(timeStart, timeEnd, sub, keyword)["data"]

entries = []

for post in posts:
  # post["body"] = post["selftext"]
  # del post["selftext"]
  entries.append(post)
  
  request_start = datetime.datetime.now()
  try:
    # the original request only got the  posts and not their comments, so,
    # for each of the posts, we do another request that fetches the 
    # comments descending from that specific post (including comments-on-comments, because that's what "link_id" gives you)
    request = requests.get('https://api.pushshift.io/reddit/search/comment/',
                          params={"link_id": post["id"], "sort": "score:desc", "size": 10})
    replies = request.json()["data"]
    ##print(replies)
  except Exception as e:
    print(f"Request exception: {e}")
    replies = []
  # Currently, it seems that Reddit rejects requests if you make more than 60 
  # requests in one minute. So to prevent that from happening, we want to
  # wait AT LEAST one second between starting each request.
  # datetime.datetime.now() gets you the current time, as a "datetime" object
  # when you subtract two of these, you get a "timedelta" object
  # on the "timedelta" object, you can call the method "total_seconds"
  # which converts it from a fancy timedelta object into a plain number of seconds.
  # That gets us the amount of time since the request started, in seconds;
  # we then want to wait for however much time is LEFT to pad it up to one second.

  elapsed = (datetime.datetime.now() - request_start).total_seconds()
  if elapsed < 1:
        sleep(1.01 - elapsed)
  

  entries.extend(replies)

for entry in entries:
      entry["permalink"] = "https://www.reddit.com"+entry["permalink"]

desired_columns = ["id","author", "title", "subreddit","num_comments", "score", "selftext", "body", "permalink"]

# from_records makes a new dataframe object, with columns determined by whatever is in "entries"
whole_df = pd.DataFrame.from_records(entries)

# we want to have desired_columns, but some of them might be missing (because, for example,
# there might not have been any comments, so none of the entries would have contained "body",
# so Pandas wouldn't have generated a "body" column). So, if any were missing, explicitly add 
# the missing ones in here:

for column_name in desired_columns:
      if column_name not in whole_df:
            whole_df[column_name] = ""

# make a new copy of the dataframe with only the desired columns in it (leaving out the others)
specific_columns_df = whole_df[desired_columns]

key = keyword.split("|")

specific_columns_df.to_csv("%s%d-%d%s.csv" %(sub,beginYear,endYear,key[0]))
##print(specific_columns_df)
