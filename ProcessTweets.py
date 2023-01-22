#!/usr/bin/env python
# coding: utf-8

# ## Initialize Spark

# In[1]:


import findspark


# In[2]:


findspark.init('C:\spark')


# In[3]:


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc


# In[4]:


sc = SparkContext()


# In[5]:


ssc = StreamingContext(sc,10)
sqlContext = SQLContext(sc)


# In[6]:


socket_stream = ssc.socketTextStream('127.0.0.1',7777)


# In[7]:


lines = socket_stream.window(20)


# ## Function to extract prices from tweets

# In[ ]:


from pyspark.sql.types import *

# Define the schema for the table
schema = StructType([
    StructField("name", StringType()),
    StructField("price", StringType())
])

# Splits the text into a list
lines.flatMap( lambda text: text.split( " " ) ) 

# checks for hashtags that match the specified price format, starting with a dollar sign and 
# consisting of one to three digits, followed by a comma or dot and three more digits, 
# followed by another comma or dot and two digits
  .filter( lambda word: word.startswith("$") and re.match(r'^\$\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})', word) )

# creates a tuple of the word and its price
  .map( lambda word: (word.split('$')[1], word) )

# create a dataframe
  .toDF(["price", "name"])

# stores the dataframe in a parquet file
  .write.format("parquet").save("crypto_prices.parquet")


# In[ ]:


#RUN --ReadTweet.py


# In[ ]:


ssc.start()


# In[10]:


ssc.stop()


# In[ ]:





# ## Unit testing

# In[ ]:


import unittest
from pyspark.sql import SparkSession

class TestCryptoPrices(unittest.TestCase):

    def setUp(self):
        # create a SparkSession
        self.spark = SparkSession.builder.appName("TestCryptoPrices").getOrCreate()

    def test_crypto_prices(self):
        # create a sample dataframe with crypto prices
        data = [("$100,000.00", "Bitcoin"), ("$200,000.00", "Ethereum"), ("$300,000.00", "Dogecoin"), ("$400,000.00", "USDT")]
        df = self.spark.createDataFrame(data, ["price", "name"])

        # call the function to process the dataframe
        processed_df = process_crypto_prices(df)

        # assert that the resulting dataframe has the correct number of rows
        self.assertEqual(processed_df.count(), 4)

        # assert that the resulting dataframe contains the expected values
        expected_data = [("100,000.00", "Bitcoin"), ("200,000.00", "Ethereum"), ("300,000.00", "Dogecoin"), ("400,000.00", "USDT")]
        for row in expected_data:
            self.assertTrue(processed_df.filter(processed_df.price == row[0]).filter(processed_df.name == row[1]).count() > 0)

    def tearDown(self):
        self.spark.stop()

if __name__ == '__main__':
    unittest.main()

