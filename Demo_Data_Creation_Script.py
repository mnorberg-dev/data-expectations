# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS `Your-Catalog`dais.trips
# MAGIC DEEP CLONE samples.nyctaxi.trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE `Your-Catalog`dais;

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

import string
import numpy as np
from faker import Faker
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, FloatType, StringType, StructType, StructField, TimestampType

@udf(returnType=IntegerType())
def alter_trip_distance():
    rng = np.random.default_rng()
    choices = ["negative", "reasonable", "long"]
    probabilities = [0.05, 0.9, 0.05]
    random_choice = rng.choice(a=choices, p=probabilities, size=1)[0]

    if random_choice == "negative":
        return int(-1 * rng.integers(0, 100, 1)[0])
    elif random_choice == "reasonable":
        return int(rng.integers(0, 20, 1)[0])
    
    return int(rng.integers(100000, 10000000, 1)[0])

@udf(returnType=StringType())
def alter_fare_amount():
    rng = np.random.default_rng()
    choices = ["negative", "reasonable", "large"]
    decimal_choices = [0, 1, 2, 5]

    probabilities = [0.05, 0.9, 0.05]
    decimal_probabilities = [0.2, 0.3, 0.4, 0.1]

    random_choice = rng.choice(a=choices, p=probabilities, size=1)[0]
    random_decimal_choice = rng.choice(a=decimal_choices, p=decimal_probabilities, size=1)[0]

    if random_choice == "negative":
        return str(round(-1 * rng.random() * 100, random_decimal_choice))
    if random_choice == "reasonable":
        return str(round(rng.random() * 100, random_decimal_choice))
    
    return str(round(10000000 + rng.random() * 1000, random_decimal_choice))

@udf(returnType=IntegerType())
def alter_pickup_zip():
    rng = np.random.default_rng()
    choices = ["small", "normal", "large"]
    probabilities = [0.05, 0.9, 0.05]
    random_choice = rng.choice(a=choices, p=probabilities, size=1)[0]

    if random_choice == "small":
        return int(rng.integers(100, 1000, 1)[0])
    if random_choice == "normal":
        return int(rng.integers(10000, 100000, 1)[0])
    
    return int(rng.integers(10000000, 100000000, 1)[0])

@udf(returnType=StringType())
def add_state_column():
    rng = np.random.default_rng()
    choices = ["missing", "normal", "invalid"]
    probabilities = [0.05, 0.9, 0.05]
    random_choice = rng.choice(a=choices, p=probabilities, size=1)[0]

    states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

    alt_states = [ 
        'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California',
        'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia',
        'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa',
        'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland',
        'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri',
        'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey',
        'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio',
        'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina',
        'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont',
        'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'
    ]
    
    if random_choice == "missing":
        return None
    
    if random_choice == "normal":
        rindex = rng.integers(0, len(states), 1)[0]
        if rng.integers(0, 2, 1)[0] == 0:
            return states[rindex]
        return alt_states[rindex]
    
    length = np.random.randint(3, 11)
    letters = string.ascii_lowercase
    return ''.join(np.random.choice(list(letters), length))

@udf(returnType=StringType())
def add_driver_column():
    rng = np.random.default_rng()
    choices = ["missing", "fake", "duplicate"]
    probabilites = [0.05, 0.9, 0.05]
    random_choice = rng.choice(a=choices, p=probabilites, size=1)[0]

    if random_choice == "missing":
        return None
    
    if random_choice == "fake":
        fake = Faker()
        return fake.name()
    
    return "Duplicate Taxi Driver"

@udf(returnType=StringType())
def add_phone_number_column():
    rng = np.random.default_rng()
    choices = ["missing", "normal", "long", "short"]
    probabilites = [0.05, 0.85, 0.05, 0.05]
    random_choice = rng.choice(a=choices, p=probabilites, size=1)[0]

    if random_choice == "missing":
        return None

    if random_choice == "normal":
        fake = Faker()
        return fake.phone_number()

    letters = string.digits

    if random_choice == "long":
        length = np.random.randint(15, 20)
        return ''.join(np.random.choice(list(letters), length))

    length = np.random.randint(2, 5)
    return ''.join(np.random.choice(list(letters), length))

@udf(returnType=StructType([
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True)
]))
def swap_pickup_and_dropoff_columns(pickup_time, dropoff_time):
    rng = np.random.default_rng()
    if rng.random() < 0.05:
        return (dropoff_time, pickup_time)
    return (pickup_time, dropoff_time)

# Register the UDF
spark.udf.register("alter_trip_distance", alter_trip_distance)
spark.udf.register("alter_fare_amount", alter_fare_amount)
spark.udf.register("alter_pickup_zip", alter_pickup_zip)
spark.udf.register("add_state_column", add_state_column)
spark.udf.register("add_driver_column", add_driver_column)
spark.udf.register("add_phone_number_column", add_phone_number_column)
spark.udf.register("swap_pickup_and_dropoff_columns", swap_pickup_and_dropoff_columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW terrible_trips_source AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   swap_pickup_and_dropoff_columns(tpep_pickup_datetime, tpep_dropoff_datetime) AS swapped_times
# MAGIC FROM
# MAGIC   trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Round function prevents SQL from adding extraneous values
# MAGIC -- Casting to float does not give you the same results AS applying round which returns a float
# MAGIC CREATE TABLE IF NOT EXISTS terrible_trips AS SELECT 
# MAGIC     swapped_times.tpep_pickup_datetime,
# MAGIC     swapped_times.tpep_dropoff_datetime,
# MAGIC     alter_trip_distance() AS trip_distance,
# MAGIC     ROUND(alter_fare_amount(), 5) AS fare_amount,
# MAGIC     alter_pickup_zip() AS pickup_zip,
# MAGIC     dropoff_zip,
# MAGIC     add_driver_column() AS driver
# MAGIC FROM terrible_trips_source;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS Taxi_Drivers AS
# MAGIC SELECT
# MAGIC   D.driver,
# MAGIC   (rand() * 150) :: INT AS age,
# MAGIC   (rand() * 50) :: INT AS years_driving,
# MAGIC   add_state_column() AS state,
# MAGIC   add_phone_number_column() AS phone_number
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       DISTINCT driver
# MAGIC     FROM
# MAGIC       terrible_trips
# MAGIC   ) D;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH driver_count AS (
# MAGIC   SELECT COUNT(*) AS count
# MAGIC   FROM taxi_drivers
# MAGIC   WHERE driver = 'Duplicate Taxi Driver'
# MAGIC ),
# MAGIC numbers AS (
# MAGIC   SELECT explode(sequence(1, 20)) AS num
# MAGIC )
# MAGIC
# MAGIC -- Insert only if the count is less than 2
# MAGIC INSERT INTO taxi_drivers
# MAGIC SELECT
# MAGIC   "Duplicate Taxi Driver" AS driver,
# MAGIC   (rand() * 150) :: INT AS age,
# MAGIC   (rand() * 50) :: INT AS years_driving,
# MAGIC   add_state_column() AS state,
# MAGIC   add_phone_number_column() AS phone_number
# MAGIC FROM numbers
# MAGIC WHERE (SELECT count FROM driver_count) < 2;