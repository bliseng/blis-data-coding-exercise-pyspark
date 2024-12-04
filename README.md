# blis-data-coding-exercise-pyspark

## Data
Input data is a time-based (epoch time in micro seconds) events-data for users, for example a user 'user1' searched google.com for leaning web development, then we will see following event record for user1 or another user 'user2' exits webstitexyz.com
```
timestamp,user_id,web_activity
1732266904142277,user1,google.com|search|learn-web-development
1732266904146398,user2,webstitexyz.com|exit
```
Here the activity column is a variable length pipe separated string.

## Requirements
1. Filter out records
   * If the activity column (`web_activity`) is an empty string or null or has special characters (except pipe, comma, %, dot, hyphen, equals and underscore). 
   * If there are two or more records for a user with the same timestamp, then all these records of the that timestamp for the user should be discarded. This is to make sure that there is only one activity for a user at a given timestamp or we discard all of them for the given timestamp. 
   
   Implement the function `filter_invalid_records` matching this requirement in [main.py](https://github.com/bliseng/blis-data-coding-exercise-pyspark)
2. Add the `activity_code` from the activity_lookup data by matching the `activity` column in the lookup data to one of the pipe-separated substrings in the activity column (`web_activity`) of the events-data. We can assume that one record from events-data will have at most one matching activity_code from the activity_lookup data. Matches have to be exact, e.g, `search` and `search1` would not be considered as exact matches. </br>Example of activity_lookup data: 
   ```
   activity,activity_code
   exit,123
   ```
   Example of the resulting events-data with the matching activity code:
   ```
   timestamp,user_id,web_activity,activity_code
   1732266904146398,user2,webstitexyz.com|exit,123
   ```
   if there is no matching keyword in the events-data with any of keywords in the lookup data then use the default code **999** for a given record. For example:
   ```
   timestamp,user_id,web_activity,activity_code
   1732266904146398,user2,webstitexyz.com|abc|zyt|url|unknown|request,999
   ```
    Implement the function `add_activity_lookup` matching this requirement in [main.py](https://github.com/bliseng/blis-data-coding-exercise-pyspark)
3. For the each calendar day find the three most active hours. The output doesn't need to be sorted in any particular way. </br>The output should look similar to this:
         
   | hour |    day     | activity_count |
   |:----:|:----------:|:--------------:|
   |  07  | 2024-12-01 |      1000      |
   |  10  | 2024-12-01 |      978       |
   |  22  | 2024-12-01 |      687       |
   |  11  | 2024-12-02 |      1500      |
   |  19  | 2024-12-02 |      1234      |
   |  23  | 2024-12-02 |      778       |
   
   Implement the function `get_top_3_active_hours` matching this requirement in [main.py](https://github.com/bliseng/blis-data-coding-exercise-pyspark)

4. Find the two top activities for each calendar day across all users. </br>The output should look similar to this:
      
   |  activity   |    day     | activity_count |
   |:-----------:|:----------:|:--------------:|
   |   search    | 2024-12-01 |      2000      |
   |   scroll    | 2024-12-01 |      779       |
   |   search    | 2024-12-02 |      1200      |
   | form-submit | 2024-12-02 |      1000      |

   Implement the function `get_top_2_activity_codes` matching this requirement in [main.py](https://github.com/bliseng/blis-data-coding-exercise-pyspark)

5. Create an activity sessions dataframe. 
   A session will be defined as a period of time where there is no time-gap of three hours or more between activity timestamps for a user. 
   If there is a gap of three hours of more then create a new session for the user. There can be more than one session per calendar day per user. 
   If a session goes across calendar days, assign the session to the date when it started. 
   </br>The output should look similar to this:
   
   | session_start_date | user  | session_time_in_mins |
   |:------------------:|:-----:|:--------------------:|
   |     2024-12-01     | user1 |          15          |
   |     2024-12-01     | user2 |          45          |
   |     2024-12-01     | user1 |          60          |
   |     2024-12-02     | user1 |          57          |
   |     2024-12-02     | user2 |         128          |

   Implement the function `get_activity_sessions_per_user` matching this requirement in [main.py](https://github.com/bliseng/blis-data-coding-exercise-pyspark)

### Things to consider
1. Code should be using pyspark (use pyspark version of your convenience).
2. Add the function implementations in main.py as stated above in the requirements.
3. The testcases in the test_app_functions.py should be running.
4. Do not change the test data.
