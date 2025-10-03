-- Databricks notebook source
CREATE OR REFRESH MATERIALIZED VIEW Terrible_Trips_DQ_SQL(
  CONSTRAINT distance_example EXPECT (trip_distance > 0)
)
AS SELECT * FROM `Your-Catalog`dais.terrible_trips