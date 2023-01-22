# etl-streaming
a quick trial of tweepy, socket tcp, and spark streaming


![image](https://user-images.githubusercontent.com/120566291/213921775-04fcacf2-5f23-4a0b-b0c5-9a486a4cb38a.png)

# **How to run:**

1. Open ProcessTweets.ipynb, run all cells unitl the commented line #Run ReadTweet.py
2. Open terminal at this point and run python ReadTweet.py (this will start the socket connection)
3. in ProcessTweets.ipynb run the cell that says ssc.start()
4. Let this run for a couple of seconds and then close the connection by running ssc.stop()

# Code Description

ReadTweet.py

*This use this code to connect to the Twitter API and stream tweets in real-time.* 

- The code authenticates the connection to the Twitter API by using the API key, API key secret, bearer token, access token, and access token secret.
- The code creates a new class called 'TweetsListener' which inherits from the 'tweepy.Stream' class and overrides the 'on_data' method. The 'on_data' method takes in a tweet as a json object, prints the 'text' field of the tweet and sends it to a client socket.
- It then creates a socket connection, binds it to the localhost IP address and port 7777. It listens for a client connection, establishes the connection and sends the tweets to the client using the 'send_tweets' function. The function uses the 'tweepy.StreamingClient' to stream tweets that match the search terms '$BTC', '$ETH', '$DOGE', and '$USDT' and filters only the 'text' field of the tweets.

ProcessTweets.ipynb

*This code is where information from tweets is extracted*

- This code initializes Spark, sets up a streaming context to listen to a socket stream on port 7777,
- processes incoming tweets to extract crypto prices, and stores the prices in a parquet file.
- It also has a unit test to check the processing function.

# ********************************Code Limitations********************************

- I was unable to get the twitter api to stream correctly due to an error with with the number of connections. see: **Tweepy StreamingClient (API v2) Error 429 - "This stream is currently at the maximum allowed connection limit."**
- I added unit tests, but could not perform them within 60 minute time frame.
- twitter has recently updataed their api from v1 to v2
