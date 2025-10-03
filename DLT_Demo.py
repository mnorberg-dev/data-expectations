# Databricks notebook source
import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# Expecatations for terrible trips

# Columns:
# tpep_pickup_datetime
# tpep_dropoff_datetime
# trip_distance
# fare_amount
# pickup_zip
# dropoff_zip
# driver

trip_rules = {}

trip_rules["trip_distance_validity"] = "trip_distance > 0 and trip_distance < 400"

trip_rules["fare_amount_validity"] = "fare_amount > 3 and fare_amount < 1500"

trip_rules["fare_amount_precision"] = "fare_amount = round(fare_amount, 2)"

trip_rules["pickup_zip_validity"] = "pickup_zip >= 10000 and pickup_zip < 100000"

trip_rules["pickup_dropoff_compliance"] = "tpep_dropoff_datetime > tpep_pickup_datetime"

trip_rules["driver_completeness"] = "driver is not null"

# COMMAND ----------

# Expectations for taxi driver

# Columns:
# driver
# age
# years_driving
# state
# phone_number

driver_rules = {}

driver_rules["driver_completeness"] = "driver is not null"
driver_rules["driver_uniqueness"] = "num_driver_entries = 1"

driver_rules["age_validity"] = "age >= 19 and age < 100"

driver_rules["phone_number_completeness"] = "phone_number is not null"

# Validity:
# Regular expression covers the following formats
# xxxxxxxxxx
# (xxx)xxx-xxxx
# (xxx) xxx - xxxx
# (xxx) xxx-xxxx
# xxx.xxx.xxxx
driver_rules[
    "phone_number_validity"
] = """
    phone_number RLIKE '^(\\\\d{10}|\\\\(\\\\d{3}\\\\)\\\\d{3}-\\\\d{4}|\\\\(\\\\d{3}\\\\) \\\\d{3} - \\\\d{4}|\\\\(\\\\d{3}\\\\) \\\\d{3}-\\\\d{4}|\\\\d{3}\\\\.\\\\d{3}\\\\.\\\\d{4})$'
    """

driver_rules["years_driving_validity"] = "years_driving < age and years_driving >= 0"

driver_rules["driver_completeness"] = "state is not null"
driver_rules[
    "state_validity"
] = "valid_us_state is not null or valid_us_state_abv is not null"

# COMMAND ----------

@dlt.table(
    name="terrible_trips_dq",
    comment="Terrible trips table with data quality expectations applied.",
)
@dlt.expect_all(trip_rules)
def terrible_trips_dq():
    res = spark.read.table("`Your-Catalog`dais.terrible_trips")
    return res


@dlt.table(
    name="taxi_driver_dq",
    comment="Taxi driver table with data quality expectations applied.",
)
@dlt.expect_all(driver_rules)
def taxi_driver_dq():
    res = spark.sql(
        """
        select
            driver.*,
            valid_us_state,
            valid_us_state_abv,
            num_driver_entries
        from
            `Your-Catalog`dais.taxi_drivers driver
        left join (
            select
                state as valid_us_state
            from
                `Your-Catalog`dais.states
        ) as vus_states on driver.state = vus_states.valid_us_state
        left join (
            select
                stateabv as valid_us_state_abv
            from
                `Your-Catalog`dais.states
        ) as vus_states_abv on driver.state = vus_states_abv.valid_us_state_abv
        left join (
            select
                driver,
                count(*) as num_driver_entries
            from
                `Your-Catalog`dais.taxi_drivers
            group by
                driver
        ) as driver_ct on driver.driver = driver_ct.driver
        """
    )
    return res

# COMMAND ----------

# # Optional Procedure For Identifying DQ Violations
# @dlt.table(
#     name="taxi_driver_dq_status",
#     comment="Will contain additional columns indicating whether a dq check passed or failed",
# )
# def taxi_driver_dq_status():
#     res = dlt.read("taxi_driver_dq")
#     for key in sorted(driver_rules.keys()):
#         res = res.withColumn(
#             key + "_passed_dq_check",
#             F.when(F.expr(driver_rules[key]), True).otherwise(False),
#         )
#     return res

# COMMAND ----------

# Optional Procedure To Quarantine Data
# quarantine_rules = "NOT({0})".format(" AND ".join(driver_rules.values()))

# @dlt.table(
#     temporary=True
# )
# def quarantine_drivers():
#     res = dlt.read("taxi_driver_dq")
#     return res.withColumn("is_quarantined", F.expr(quarantine_rules))

# @dlt.table()
# def quarantined_drivers():
#     res = dlt.read("quarantine_drivers")
#     return res.filter("is_quarantined=true")

# @dlt.table()
# def valid_drivers():
#     res = dlt.read("quarantine_drivers")
#     return res.filter("is_quarantined=false")

# COMMAND ----------

# @dlt.table()
# def quarantine_subset():
#     res = dlt.read("taxi_driver_dq_status")
#     return res.filter(
#         (F.col("age_validity_passed_dq_check") == False)
#         | (F.col("state_validity_passed_dq_check") == False)
#     )

# @dlt.table()
# def valid_subset():
#     res = dlt.read("taxi_driver_dq_status")
#     return res.filter(
#         (F.col("age_validity_passed_dq_check") == True)
#         & (F.col("state_validity_passed_dq_check") == True)
#     )