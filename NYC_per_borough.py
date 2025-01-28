from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, avg, stddev, year, when, first, sum as spark_sum, regexp_replace, count

spark = SparkSession.builder    \
    .appName("Data Analysis")   \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

fire_incident_data_NY  = spark.read.csv("/user/s2186047/project/NY-Fire-Incidents.csv", header = True, inferSchema = True)
firehouse_listings_data_NY = spark.read.csv("/user/s2186047/project/NY-Firehouse-Listing.csv", header=True, inferSchema=True)
traffic_volume_data = spark.read.csv("/user/s2186047/project/NY-Automated-Traffic-Volume-Counts.csv", header=True, inferSchema=True)

# Fire incident preprocessing
filteredColumns = fire_incident_data_NY.select("IM_INCIDENT_KEY", "INCIDENT_TYPE_DESC", "INCIDENT_DATE_TIME", "ARRIVAL_DATE_TIME", "LAST_UNIT_CLEARED_DATE_TIME", "BOROUGH_DESC", "HIGHEST_LEVEL_DESC")

# data cleaning
filteredColumns = filteredColumns.filter(
    (filteredColumns["BOROUGH_DESC"] == "1 - Manhattan") | 
    (filteredColumns["BOROUGH_DESC"] == "2 - Bronx") | 
    (filteredColumns["BOROUGH_DESC"] == "3 - Staten Island") | 
    (filteredColumns["BOROUGH_DESC"] == "4 - Brooklyn") | 
    (filteredColumns["BOROUGH_DESC"] == "5 - Queens")
)

#Check that all rows are complete
filteredColumns = filteredColumns.filter(
    filteredColumns["IM_INCIDENT_KEY"].isNotNull() & 
    filteredColumns["INCIDENT_TYPE_DESC"].isNotNull() & 
    filteredColumns["INCIDENT_DATE_TIME"].isNotNull() & 
    filteredColumns["ARRIVAL_DATE_TIME"].isNotNull() & 
    filteredColumns["LAST_UNIT_CLEARED_DATE_TIME"].isNotNull() & 
    filteredColumns["HIGHEST_LEVEL_DESC"].isNotNull()
)

# filteredColumns.printSchema()
# filteredColumns.show(5)

#Define alarm types (HIGHEST_LEVEL_DESC > alarm_group)
#signal 7-5 is huge
filteredColumns = filteredColumns.withColumn(
    "alarm_group",
    when(filteredColumns["HIGHEST_LEVEL_DESC"].isin(
        "0 - Initial alarm", "00 - Complaint/Still"), "Initial Alarm")
    .when(filteredColumns["HIGHEST_LEVEL_DESC"].isin(
        "1 - More than initial alarm, less than Signal 7-5", "11 - First Alarm", "2 - 2nd alarm", "22 - Second Alarm"), "Standard Alarm")
    .when(filteredColumns["HIGHEST_LEVEL_DESC"].isin(
        "7 - Signal 7-5", "75 - All Hands Working", "3 - 3rd alarm", "33 - Third Alarm", "4 - 4th alarm", "44 - Fourth Alarm", "5 - 5th alarm", "55 - Fifth Alarm", "66 - Sixth Alarm", "77 - Seventh Alarm", "88 - Eighth Alarm"), "Critical Alarm")
    .otherwise("Unclassified Alarm")  # For any other unclassified alarms
)
#Define column order for alarm_group
alarm_order = ["Initial Alarm", "Standard Alarm", "Moderate Alarm", "Critical Alarm", "Maximum Alarm", "Unclassified Alarm"]

#Turn INCIDENT_DATE_TIME & ARRIVAL_DATE_TIME into usable variables
#Create a new column for the calculated response_time_seconds
for date_column in ["INCIDENT_DATE_TIME", "ARRIVAL_DATE_TIME"]:
    filteredColumns = filteredColumns.withColumn(date_column, to_timestamp(date_column, "MM/dd/yyyy hh:mm:ss a"))

# filteredColumns = filteredColumns.withColumn("INCIDENT_DATE_TIME-test", to_timestamp("INCIDENT_DATE_TIME", "MM/dd/yyyy hh:mm:ss a"))
# filteredColumns = filteredColumns.withColumn("ARRIVAL_DATE_TIME", to_timestamp("ARRIVAL_DATE_TIME", "MM/dd/yyyy hh:mm:ss a"))
filteredColumns = filteredColumns.withColumn("response_time_seconds", 
                   (unix_timestamp("ARRIVAL_DATE_TIME") - unix_timestamp("INCIDENT_DATE_TIME")))

# Add year column
filteredColumns = filteredColumns.withColumn("year", year("INCIDENT_DATE_TIME"))

# filter data which is not in the range 01 Jan 2023 till 01 Jan 2024
start_year = 2013
end_year = 2023
filteredColumns = filteredColumns.filter(
    col("year").between(start_year, end_year)
)

# Calculate the IQR to remove outliers from response_time_seconds

# Step 1: Calculate Q1 (25th percentile), Q3 (75th percentile), and IQR
quantiles = filteredColumns.approxQuantile("response_time_seconds", [0.25, 0.75], 0.0)
Q1 = quantiles[0]  # 25th percentile
Q3 = quantiles[1]  # 75th percentile
IQR = Q3 - Q1       # Interquartile Range

# Step 2: Define the outlier thresholds
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

# Step 3: Filter out the outliers based on the IQR method
filteredColumns = filteredColumns.filter(
    (filteredColumns["response_time_seconds"] >= lower_bound) & 
    (filteredColumns["response_time_seconds"] <= upper_bound)
)

# Firestation data preprocessing
fireStationCounts = firehouse_listings_data_NY.groupBy("Borough").agg(
  count("Borough").alias("Number of Stations")
)

#ADDED TO MATCH WITH TRAFFIC VOLUME DATA (as prefix 1. - , 2. -, etc. are not present in that dataset)
fire_incident_data_preprocessed = filteredColumns.withColumn("BOROUGH_DESC", regexp_replace("BOROUGH_DESC", "^\d+ - ", ""))

# Calculate mean and standard deviation of response times per borough and alarm group
stats_per_borough_alarm = fire_incident_data_preprocessed.groupBy("BOROUGH_DESC", "alarm_group") \
                                         .agg(
                                             avg("response_time_seconds").alias("mean_response_time"),
                                             stddev("response_time_seconds").alias("stddev_response_time")
                                         )
stats_per_borough_alarm = stats_per_borough_alarm.orderBy("BOROUGH_DESC", "alarm_group")
stats_per_borough_alarm = stats_per_borough_alarm.join(fireStationCounts, stats_per_borough_alarm["BOROUGH_DESC"]==fireStationCounts["Borough"], "left")
stats_per_borough_alarm = stats_per_borough_alarm.drop("Borough")
stats_per_borough_alarm.show(truncate=False)

# Traffic volume data preprocessing
grouped_traffic_volume = traffic_volume_data \
        .filter(col("Yr").between(start_year, end_year)) \
        .groupby(["Boro", "Yr"]).agg(spark_sum("Vol").alias("traffic_volume_counts"))



# Calculate mean and standard deviation per borough and alarm group
grouped_fire_incident_data = fire_incident_data_preprocessed.groupBy("year", "BOROUGH_DESC", "alarm_group") \
                    .agg(
                        avg("response_time_seconds").alias("mean_response_time"),
                        stddev("response_time_seconds").alias("stddev_response_time")
                    )

# Create separate tables for each year (2013–2023)
for year in range(start_year, end_year + 1): #exclusive end
    yearly_fire_incident_data = grouped_fire_incident_data.filter(grouped_fire_incident_data["year"] == year)
    
    # Pivot the table to match the design
    pivot_table = yearly_fire_incident_data.groupBy("BOROUGH_DESC").pivot("alarm_group", ["Initial Alarm", "Standard Alarm", "Critical Alarm"]) \
                       .agg(
                           first("mean_response_time").alias("mean"),
                           first("stddev_response_time").alias("stddev")
                       )
    
    grouped_yearly_traffic_data = grouped_traffic_volume.filter(grouped_traffic_volume["Yr"] == year).select(["Boro", "traffic_volume_counts"])

    yearly_data = pivot_table.join(grouped_yearly_traffic_data, pivot_table["BOROUGH_DESC"] == grouped_yearly_traffic_data["Boro"], "left")
    yearly_data = yearly_data.drop("Boro")
    print(f"Year: {year}")
    print("new way")
    yearly_data.show(truncate=False)

# Stop SparkSession
spark.stop()