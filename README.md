# blis-data-coding-exercise-pyspark

## Data
Input data is a time-based (epoch time) event data for users, for example a user 'user1' searched google.com for leaning web development, then we will see following event record for user1 or another user 'user2' exits webstitexyz.com
```
timestamp,user_id,web_activity
1732266904142277,user1,google.com|search|learn-web-development
1732266904146398,user2,webstitexyz.com|exit
```
Here the activity column is a variable length pipe separated string.

## Requirements
1. Filter out records if the activity column has special chars (except pipe, comma, %, dot, hyphen and underscore) or empty string. If there are two or more records for a user for a same timestamp, then all these records of the given timestamp for the user should also be discarded. This is to make sure that at a given timestamp no more than 1 activity for a given user. 
   Implement the function `filter_invalid_records` for this requirement in [main.py](https://github.com/bliseng/blis-data-coding-exercise-pyspark)
2. Add activity code using a given lookup file by matching the activity-keyword in lookup file to one of pipe-separated string in activity column of event-data-file, which will for example have an entry like below for activity exit -
   ```
   activity,activity_code
   exit,123
   ```
   the output record for matching activity should look like -
   ```
   timestamp,user_id,web_activity,activity_code
   1732266904146398,user2,webstitexyz.com|exit,123
   ```
   if there is no matching keyword in the event-data-file with that of keywords in the lookup file then use the default code **999** for the given record, example -
   ```
   timestamp,user_id,web_activity,activity_code
   1732266904146398,user2,webstitexyz.com|abc|zyt|url|unknown|request,999
   ```
    Implement the function `add_activity_lookup` for this requirement in [main.py](https://github.com/bliseng/blis-data-coding-exercise-pyspark)
3. Find out the top 3 active hours of the day. The example output format -
         
   | hour |    day     | activity_count |
   |:----:|:----------:|:--------------:|
   |  07  | 2024-12-01 |      1000      |
   |  10  | 2024-12-01 |      978       |
   |  22  | 2024-12-02 |      687       |
   |  11  | 2024-12-02 |      1500      |
   |  19  | 2024-12-02 |      1234      |
   |  23  | 2024-12-02 |      778       |
   
   Implement the function `get_top_3_active_hours` for this requirement in [main.py](https://github.com/bliseng/blis-data-coding-exercise-pyspark)

4. Find out top 2 activities of the day across users. The example output format -
      
   |  activity   |    day     | activity_count |
   |:-----------:|:----------:|:--------------:|
   |   search    | 2024-12-01 |      2000      |
   |   scroll    | 2024-12-01 |      779       |
   |   search    | 2024-12-02 |      1200      |
   | form-submit | 2024-12-02 |      1000      |

   Implement the function `get_top_2_activity_codes` for this requirement in [main.py](https://github.com/bliseng/blis-data-coding-exercise-pyspark)

5. Find out the activity session (in mins) of each user, assuming no session can go longer than 3 hours. A session is defined as a continuous period where a user is observed to do different activities. The example output format -
   
   | session_time_in_mins |    day     | user  |
   |:--------------------:|:----------:|:-----:|
   |          15          | 2024-12-01 | user1 |
   |          45          | 2024-12-01 | user2 |
   |          60          | 2024-12-02 | user1 |
   |         128          | 2024-12-02 | user2 |

   Implement the function `get_activity_sessions_per_user` for this requirement in [main.py](https://github.com/bliseng/blis-data-coding-exercise-pyspark)

### Things to consider
1. Code should be using pyspark (use pyspark version of your convenience).
2. Add the function implementations in main.py as stated above in the requirements.
3. The testcases in the test_app_functions.py should be running.
4. Do not change the test data.
