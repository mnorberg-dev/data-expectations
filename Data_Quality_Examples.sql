-- Databricks notebook source
-- Storing all the code that was used to generate tables which are displayed in the presentation
-- This includes pictures of the tables with the demo data and pictures of data quality issues

-- COMMAND ----------

USE `Your-Catalog`dais

-- COMMAND ----------

-- Original Trips Table
select * from trips

-- COMMAND ----------

-- Terrible Trips Table
select * from terrible_trips

-- COMMAND ----------

-- Taxi Drivers Table
select * from taxi_drivers

-- COMMAND ----------

-- Completeness Issue
select * from terrible_trips
where driver is null
limit 5

-- COMMAND ----------

-- Uniqueness Issue
select 
  driver,
  count(*) as driver_count
from
  taxi_drivers
group by driver
order by driver_count desc
limit 5

-- COMMAND ----------

-- Validity Issue
select * from terrible_trips
where len(pickup_zip) > 5
limit 5

-- COMMAND ----------

-- Precision Issue
select * from terrible_trips
where fare_amount != round(fare_amount, 2)
limit 5

-- COMMAND ----------

-- Accuracy Issue
select * from taxi_drivers
where age > 100
order by age desc
limit 5

-- COMMAND ----------

-- Consistency Issue
select * from taxi_drivers
where driver = 'Duplicate Taxi Driver'
limit 5

-- COMMAND ----------

-- Schema after adding data expectation result columns
DESCRIBE `Your-Catalog`dais.taxi_driver_dq_status

-- COMMAND ----------

-- Find rows that passed state validity
select * from taxi_driver_dq_status where state_validity_passed_dq_check = False

-- COMMAND ----------

-- Quarantine image
select * from valid_drivers

-- COMMAND ----------

-- Expectation SQL Relationship Negation Example
select * from taxi_drivers 
where age <= 0

-- COMMAND ----------

-- Query Event Log To Get Data Quality Stats
CREATE
OR REPLACE TEMP VIEW event_log_raw AS
SELECT
  *
FROM
  event_log("Your DLT Pipeline Id");
  
CREATE
OR REPLACE TEMP VIEW latest_update AS
SELECT
  origin.update_id AS id
FROM
  event_log_raw
WHERE
  event_type = 'create_update'
ORDER BY
  timestamp DESC
LIMIT
  1;

-- COMMAND ----------

-- View Data Quality Results
SELECT
  row_expectations.dataset as dataset,
  row_expectations.name as expectation,
  SUM(row_expectations.passed_records) as passing_records,
  SUM(row_expectations.failed_records) as failing_records
FROM
  (
    SELECT
      explode(
        from_json(
          details :flow_progress :data_quality :expectations,
          "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
        )
      ) row_expectations
    FROM
      event_log_raw,
      latest_update
    WHERE
      event_type = 'flow_progress'
      AND origin.update_id = latest_update.id
  )
GROUP BY
  row_expectations.dataset,
  row_expectations.name

-- COMMAND ----------

select * from quarantine_subset

-- COMMAND ----------

select * from quarantine_subset
where age_validity_passed_dq_check = True

-- COMMAND ----------

select * from valid_subset