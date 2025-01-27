from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, avg, stddev, year, when, first, count

# Initialize SparkSession
spark = SparkSession.builder    \
    .appName("Zipcode Analysis")   \
    .getOrCreate()

# Load data
data = spark.read.csv("file:///home/s2106841/New York Dataset Fire Department Response Time.csv", header=True, inferSchema=True)
data2 = spark.read.csv("file:////home/s2106841/FDNY_Firehouse_Listing.csv", header=True, inferSchema=True)

# Filter relevant columns (assuming 'ZIPCODE' and 'TOTAL_INCIDENT_DURATION' are part of the dataset)
filteredColumns = data.select("IM_INCIDENT_KEY", "INCIDENT_DATE_TIME", "ARRIVAL_DATE_TIME", "LAST_UNIT_CLEARED_DATE_TIME", "BOROUGH_DESC", "ZIP_CODE")


# Turn INCIDENT_DATE_TIME, ARRIVAL_DATE_TIME & LAST_UNIT_CLEARED_DATE_TIME into usable variables
filteredColumns = filteredColumns.withColumn("INCIDENT_DATE_TIME", to_timestamp("INCIDENT_DATE_TIME", "MM/dd/yyyy HH:mm:ss a"))
filteredColumns = filteredColumns.withColumn("ARRIVAL_DATE_TIME", to_timestamp("ARRIVAL_DATE_TIME", "MM/dd/yyyy HH:mm:ss a"))
filteredColumns = filteredColumns.withColumn("LAST_UNIT_CLEARED_DATE_TIME", to_timestamp("LAST_UNIT_CLEARED_DATE_TIME", "MM/dd/yyyy HH:mm:ss a"))

# Create a new column for the calculated response_time_seconds & handling_time_seconds
filteredColumns = filteredColumns.withColumn("response_time_seconds", 
                   (unix_timestamp("ARRIVAL_DATE_TIME") - unix_timestamp("INCIDENT_DATE_TIME")))
filteredColumns = filteredColumns.withColumn("handling_time_seconds", 
                   (unix_timestamp("LAST_UNIT_CLEARED_DATE_TIME") - unix_timestamp("ARRIVAL_DATE_TIME")))

# Remove rows with null values in the relevant columns
filteredColumns = filteredColumns.filter(
    filteredColumns["ZIP_CODE"].isNotNull() &
    filteredColumns["response_time_seconds"].isNotNull() &
    filteredColumns["handling_time_seconds"].isNotNull()
)
# Firestation data
fireStationCounts = data2.groupBy("Postcode").agg(
    count("Postcode").alias("Number of Stations")
)
fireStationCounts = fireStationCounts.withColumnRenamed("Postcode", "ZIP_CODE")


# Group by ZIPCODE and calculate metrics
zipcode_stats = filteredColumns.groupBy("ZIP_CODE") \
    .agg(
        avg("response_time_seconds").alias("avg (Response Time)"),
        stddev("response_time_seconds").alias("stddev (Response Time)"),
        avg("handling_time_seconds").alias("avg (Handling Time)"),
        stddev("handling_time_seconds").alias("stddev (Handling Time)"),
        count("IM_INCIDENT_KEY").alias("Incident Count")
    )

# Filter low incident counts (<1000)
zipcode_stats = zipcode_stats.filter(col("Incident Count") >= 1000)

# Join fireStationCounts to table
final_table = zipcode_stats.join(fireStationCounts, on="ZIP_CODE", how="full")
                   
# Order the results by ZIPCODE
final_table = final_table.orderBy("ZIP_CODE")

# Show the resulting table
final_table.show(truncate=False)

# Stop SparkSession
spark.stop()
