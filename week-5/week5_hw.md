### Question 1: 

**Install Spark and PySpark** 

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?
- 3.3.2
- 2.1.4
- 1.2.3
- 5.4
</br></br>

>>> spark.version
'3.3.2'


### Question 2: 

**HVFHW June 2021**

Read it with Spark using the same schema as we did in the lessons.</br> 
We will use this dataset for all the remaining questions.</br>
Repartition it to 12 partitions and save it to parquet.</br>
What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.</br>


- 2MB
- 24MB
- 100MB
- 250MB
</br></br>

!ls -lh fhvhv/2021/06/
total 272M
-rw-r--r-- 1 jtu jtu   0 Feb 27 17:49 _SUCCESS
-rw-r--r-- 1 jtu jtu 23M Feb 27 17:49 part-00000-8a05a47a-4cfe-4a09-b3c7-23eb59b3d0e1-c000.snappy.parquet
-rw-r--r-- 1 jtu jtu 23M Feb 27 17:49 part-00001-8a05a47a-4cfe-4a09-b3c7-23eb59b3d0e1-c000.snappy.parquet
-rw-r--r-- 1 jtu jtu 23M Feb 27 17:49 part-00002-8a05a47a-4cfe-4a09-b3c7-23eb59b3d0e1-c000.snappy.parquet
-rw-r--r-- 1 jtu jtu 23M Feb 27 17:49 part-00003-8a05a47a-4cfe-4a09-b3c7-23eb59b3d0e1-c000.snappy.parquet
-rw-r--r-- 1 jtu jtu 23M Feb 27 17:49 part-00004-8a05a47a-4cfe-4a09-b3c7-23eb59b3d0e1-c000.snappy.parquet
-rw-r--r-- 1 jtu jtu 23M Feb 27 17:49 part-00005-8a05a47a-4cfe-4a09-b3c7-23eb59b3d0e1-c000.snappy.parquet
-rw-r--r-- 1 jtu jtu 23M Feb 27 17:49 part-00006-8a05a47a-4cfe-4a09-b3c7-23eb59b3d0e1-c000.snappy.parquet
-rw-r--r-- 1 jtu jtu 23M Feb 27 17:49 part-00007-8a05a47a-4cfe-4a09-b3c7-23eb59b3d0e1-c000.snappy.parquet
-rw-r--r-- 1 jtu jtu 23M Feb 27 17:49 part-00008-8a05a47a-4cfe-4a09-b3c7-23eb59b3d0e1-c000.snappy.parquet
-rw-r--r-- 1 jtu jtu 23M Feb 27 17:49 part-00009-8a05a47a-4cfe-4a09-b3c7-23eb59b3d0e1-c000.snappy.parquet
-rw-r--r-- 1 jtu jtu 23M Feb 27 17:49 part-00010-8a05a47a-4cfe-4a09-b3c7-23eb59b3d0e1-c000.snappy.parquet
-rw-r--r-- 1 jtu jtu 23M Feb 27 17:49 part-00011-8a05a47a-4cfe-4a09-b3c7-23eb59b3d0e1-c000.snappy.parquet

### Question 3: 

**Count records**  

How many taxi trips were there on June 15?</br></br>
Consider only trips that started on June 15.</br>

- 308,164
- 12,856
- 452,470
- 50,982
</br></br>

from pyspark.sql.functions import date_format

df.filter(date_format(df.pickup_datetime, 'yyyy-MM-dd') == '2021-06-15').count()

452470

### Question 4: 

**Longest trip for each day**  

Now calculate the duration for each trip.</br>
How long was the longest trip in Hours?</br>

- 66.87 Hours
- 243.44 Hours
- 7.68 Hours
- 3.32 Hours
</br></br>

from pyspark.sql.functions import col, unix_timestamp, desc

df.withColumn("duration_hours", (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 3600) \
    .orderBy(desc("duration_hours")) \
    .show(5)

+--------------------+-------------------+-------------------+------------+------------+-------+----------------------+------------------+
|dispatching_base_num|    pickup_datetime|   dropoff_datetime|PULocationID|DOLocationID|SR_Flag|Affiliated_base_number|    duration_hours|
+--------------------+-------------------+-------------------+------------+------------+-------+----------------------+------------------+
|              B02872|2021-06-25 13:55:41|2021-06-28 08:48:25|          98|         265|      N|                B02872|  66.8788888888889|
|              B02765|2021-06-22 12:09:45|2021-06-23 13:42:44|         188|         198|      N|                B02765|25.549722222222222|
|              B02879|2021-06-27 10:32:29|2021-06-28 06:31:20|          78|         169|      N|                B02879|19.980833333333333|
|              B02800|2021-06-26 22:37:11|2021-06-27 16:49:01|         263|          36|      N|                  null|18.197222222222223|
|              B02682|2021-06-23 20:40:43|2021-06-24 13:08:44|           3|         247|      N|                B02682|16.466944444444444|
+--------------------+-------------------+-------------------+------------+------------+-------+----------------------+------------------+

66.87 hours

### Question 5: 

**User Interface**

 Spark’s User Interface which shows application's dashboard runs on which local port?</br>

- 80
- 443
- 4040
- 8080
</br></br>

4040
23/02/27 17:47:53 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.


### Question 6: 

**Most frequent pickup location zone**

Load the zone lookup data into a temp view in Spark</br>
[Zone Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv)</br>

Using the zone lookup data and the fhvhv June 2021 data, what is the name of the most frequent pickup location zone?</br>

- East Chelsea
- Astoria
- Union Sq
- Crown Heights North
</br></br>

df.join(
    df_zones, df.PULocationID == df_zones.LocationID, "inner"
) \
.groupBy("Zone") \
.agg( {"*": "count"} ) \
.withColumnRenamed("count(1)", "count") \
.orderBy(desc("count")) \
.show()

+--------------------+------+
|                Zone| count|
+--------------------+------+
| Crown Heights North|231279|
|        East Village|221244|
|         JFK Airport|188867|
|      Bushwick South|187929|
|       East New York|186780|
|TriBeCa/Civic Center|164344|
|   LaGuardia Airport|161596|
|            Union Sq|158937|
|        West Village|154698|
|             Astoria|152493|
|     Lower East Side|151020|
|        East Chelsea|147673|
|Central Harlem North|146402|
|Williamsburg (Nor...|143683|
|          Park Slope|143594|
|  Stuyvesant Heights|141427|
|        Clinton East|139611|
|West Chelsea/Huds...|139431|
|             Bedford|138428|
|         Murray Hill|137879|
+--------------------+------+
only showing top 20 rows


## Submitting the solutions

* Form for submitting: https://forms.gle/EcSvDs6vp64gcGuD8
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 06 March (Monday), 22:00 CET


## Solution

We will publish the solution here