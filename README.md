# RedditParser
Python script for parsing reddit  
prints out postst, comments, score(upvote), title,
           link to the post, number of comments under a post,
           post id, auther and subreddit in to a csv file 
           called reddit_data.csv

For sample out put download reddit_data.csv file and open in Microsoft Excel

# Instructions
Running `python3 get_reddit_data.py -h` will output how to run this script. This script will require at least one JSON file as input in order to run. The format of the JSON file has to be in this form:
```json
{
  "beginYear": 2018,
  "beginMonth": 1,
  "beginDay": 30,
  "endYear": 2019,
  "endMonth": 1,
  "endDay": 30,
  "sub": "UberDrivers",
  "keyword": "sad",
  "csvFileToSave": "data.csv"
}
```
where each field can be changed accordingly. An example of running this script, assuming that the above contents are in some file called `input.json` and this file is in the same directory, run `python3 get_reddit_data.py input.json`. This script can also take multiple JSON files as input: `python3 get_reddit_data.py input1.json input2.json input3.json...`. 

