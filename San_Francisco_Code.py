# Code for creating the San Francisco table

from pyspark.sql import SparkSession

# Create spark session
spark = SparkSession.builder    \
    .appName("Data Analysis")   \
    .getOrCreate()

# Import necessary functions
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, avg, when, first, count, stddev, countDistinct

data = spark.read.csv("/user/s2502720/San_Francisco_Fire_Incidents.csv", header = True, inferSchema = True)

# Make sure timestamps are formatted as datetimes
data = data.withColumn("Alarm Time", to_timestamp("Alarm DtTm", "yyyy/MM/dd hh:mm:ss a"))
data = data.withColumn("Arrival Time", to_timestamp("Arrival DtTm", "yyyy/MM/dd hh:mm:ss a"))
data = data.withColumn("Close Time", to_timestamp("Close DtTm", "yyyy/MM/dd hh:mm:ss a"))
data = data.drop(col("Alarm DtTm"), col("Arrival DtTm"), col("Close DtTm"), col("Incident Date"))

# Calculate Response and Handling times
data = data.withColumn("Response Time", (unix_timestamp("Arrival Time") - unix_timestamp("Alarm Time")))
data = data.withColumn("Handling Time", (unix_timestamp("Close Time") - unix_timestamp("Arrival Time")))
data = data.withColumn("Incident Duration", (unix_timestamp("Close Time") - unix_timestamp("Alarm Time")))

# Filter empty rows
data = data.filter(
    data['Response Time'].isNotNull() &
    data['Handling Time'].isNotNull() &
    data['Incident Duration'].isNotNull() &
    (data['Response Time'] > 0) &
    (data['Handling Time'] > 0) &
    (data['Incident Duration'] > 0)
)


# Calculate the IQR to remove outliers from Response Time

# Step 1: Calculate Q1 (25th percentile), Q3 (75th percentile), and IQR
quantiles = data.approxQuantile("Response Time", [0.25, 0.75], 0.0)
Q1 = quantiles[0]  # 25th percentile
Q3 = quantiles[1]  # 75th percentile
IQR = Q3 - Q1       # Interquartile Range

# Step 2: Define the outlier thresholds
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

# Step 3: Filter out the outliers based on the IQR method
data = data.filter(
    (data["Response Time"] >= lower_bound) &
    (data["Response Time"] <= upper_bound)
)


# Calculate the IQR to remove outliers from Handling Time

# Step 1: Calculate Q1 (25th percentile), Q3 (75th percentile), and IQR
quantiles = data.approxQuantile("Handling Time", [0.25, 0.75], 0.0)
Q1 = quantiles[0]  # 25th percentile
Q3 = quantiles[1]  # 75th percentile
IQR = Q3 - Q1       # Interquartile Range

# Step 2: Define the outlier thresholds
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

# Step 3: Filter out the outliers based on the IQR method
averageTimes = data.filter(
    (data["Handling Time"] >= lower_bound) &
    (data["Handling Time"] <= upper_bound)
)


# Load all fire station locations
fireStations = spark.createDataFrame([
    (1, 94103),
    (2, 94133),
    (3, 94109),
    (4, 94158),
    (5, 94115),
    (6, 94114),
    (7, 94110),
    (8, 94107),
    (9, 94124),
    (10, 94118),
    (11, 94131),
    (12, 94117),
    (13, 94111),
    (14, 94121),
    (15, 94112),
    (16, 94123),
    (17, 94124),
    (18, 94116),
    (19, 94132),
    (20, 94131),
    (21, 94117),
    (22, 94122),
    (23, 94122),
    (24, 94114),
    (25, 94124),
    (26, 94131),
    (28, 94133),
    (29, 94103),
    (31, 94118),
    (32, 94110),
    (33, 94112),
    (34, 94121),
    (35, 94105),
    (36, 94102),
    (37, 94107),
    (38, 94115),
    (39, 94127),
    (40, 94116),
    (41, 94109),
    (42, 94134),
    (43, 94112),
    (44, 94134),
    (48, 94130),
    (49, 94124),
    (51, 94129)
], ["Fire Station", "zipcode"])

# Count all the fire stations per zipcode
fireStationCounts = fireStations.groupBy("zipcode").agg(
    count("Fire Station").alias("Number of Stations")
)

# Calculate the average Response Time for every zipcode
zipcodeTimes = averageTimes.groupBy("zipcode").agg(
        avg("Response Time"),
        stddev("Response Time"),
        avg("Handling Time"),
        stddev("Handling Time"),
        count("Response Time").alias("Incident Count")
    )

zipcodeTimes = zipcodeTimes.filter(col('zipcode').isNotNull() & (col('Incident Count') > 1000))

# Join response and handling times with fire station counts
zipcodeTimes = zipcodeTimes.join(fireStationCounts, 'zipcode', 'full')


# Check for significance
zipcodeTime = zipcodeTimes.agg(avg('Avg Response Time'), stddev(' AvgResponse Time'))


spark.stop
