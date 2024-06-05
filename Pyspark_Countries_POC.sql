POC: 
import requests
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.types import *
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql.functions import regexp_replace



# Specify the URL of the REST API endpoint you want to access
url = "https://restcountries.com/v3.1/all"

# Make a GET request to the API endpoint
response = requests.get(url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Extract the JSON data from the response
    data = response.json()
    # Now you can work with the data as needed
    # print(data)
else:
    # Print an error message if the request was not successful
    print("Error:", response.status_code)


# Create a Pandas DataFrame from the data
df = pd.DataFrame(data)


# Specify the path where you want to save the CSV file
csv_file_path = "output.csv"

# Write the DataFrame to a CSV file
df.to_csv(csv_file_path, index=False)

print(f"CSV file '{csv_file_path}' has been created with the JSON data.")


# Load the CSV data into a Pandas DataFrame
df = pd.read_csv("output.csv")

#converting pandas dataframe to pyspark dataframe
sparkDF=spark.createDataFrame(df)

# sparkDF.printSchema()


sparkDF=sparkDF.withColumn('latitude', split(sparkDF['latlng'], ',').getItem(0)) \
       .withColumn('longitude', split(sparkDF['latlng'], ',').getItem(1))\


#removing special characters from all columns

sparkDF=sparkDF.withColumn('latitude',regexp_replace(col("latitude"), "[\[\]'\"']", ""))\
    .withColumn('longitude',regexp_replace(col("longitude"), "[\[\]'\"']", ""))\
            .withColumn("capital",regexp_replace(col("capital"), "[\[\]'\"']", ""))\
                .withColumn("tld",regexp_replace(col("tld"), "[\[\]'\"']", ""))\
                    .withColumn("borders",regexp_replace(col("borders"), "[\[\]'\"']", ""))\
                        .withColumn("continents",regexp_replace(col("continents"), "[\[\]'\"']", ""))\
        .drop("latlng")


sparkDF=sparkDF.drop("name","altSpellings","maps","translations","capitalInfo","demonyms")


from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField
new_sparkDF=sparkDF.withColumnRenamed("tld","country_domain_name")\
    .withColumnRenamed("cca2","Two_letter_ISO_country_code")\
        .withColumnRenamed("cca3","Three_letter_ISO_country_code")\
            .withColumnRenamed("ccn3","three_digit_country_code")\
                .withColumnRenamed("cioc","International_Olympic_Committee_country_code")\
                    .drop("cca2","cca3","ccn3","cioc")

new_sparkDF.createOrReplaceTempView("test")
display(spark.sql("select * from test"))

display(new_sparkDF.groupBy("region")\
    .count())
	
display(new_sparkDF.groupBy("region")\
    .sum("population"))
	
new_sparkDF.write.format("parquet").mode("overwrite").saveAsTable("countries")




1) How to sum two columns data using sql?

columns=["id","name","roll","m1","m2"]
data=[(1,"salaja",1001,10,23),
      (2,"naresh",1002,24,39)]
df=spark.createDataFrame(data=data,schema=columns)
import pyspark.sql.functions as f
from pyspark.sql.types import *
def sum_fun(m1,m2):
      total_sum=m1+m2
      return total_sum
new_f = f.udf(sum_fun, IntegerType())


df.withColumn("total",new_f("m1","m2")).show()

df.select("*",(df["m1"]+df["m2"]).alias("total")).show()

df.createOrReplaceTempView('test')

%sql
select id,name,roll,m1,m2,sum(m1+m2) from test
group by id,name,roll,m1,m2;

2)how to drop the entire record if any null value?

schema=['id','name','roll']
data=[(None,'abc',3),
(10,'xyz',4),
(20,None,None),
(30,'pqr',None)]
test_df=spark.createDataFrame(data=data,schema=schema)
test_df.na.drop().show()

3) how to move all the zeros to end in the list?

# initializing a list
numbers = [1, 3, 0, 4, 0, 5, 6, 0, 7]
# moving all the zeroes to end
new_list = [num for num in numbers if num != 0] + [num for num in numbers if num == 0]

second method
numbers = [1, 3, 0, 4, 0, 5, 6, 0, 7]

lst=[]
lst1=[]
for num in numbers:
    if num !=0:
        lst.append(num)
    else:
        lst1.append(num)
        
print(lst+lst1)

# printing the new list
print(new_list)
[1, 3, 4, 5, 6, 7, 0, 0, 0] 

4) How to return second highest number in the list?

random_numbers = [2,5,6,1,8,3] 
sorted(random_numbers)[-2]