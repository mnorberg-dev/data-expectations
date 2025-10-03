# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC USE `Your-Catalog`dais; 
# MAGIC
# MAGIC -- DEFINITION OF STATES TABLE
# MAGIC CREATE TABLE IF NOT EXISTS States (
# MAGIC     State STRING,
# MAGIC     StateABV STRING
# MAGIC );
# MAGIC
# MAGIC -- TEMPORARY TABLE FOR NEW VALUES
# MAGIC CREATE OR REPLACE TEMP VIEW NewStates AS
# MAGIC SELECT * FROM VALUES
# MAGIC ('Alabama', 'AL'),
# MAGIC ('Alaska', 'AK'),
# MAGIC ('Arizona', 'AZ'),
# MAGIC ('Arkansas', 'AR'),
# MAGIC ('California', 'CA'),
# MAGIC ('Colorado', 'CO'),
# MAGIC ('Connecticut', 'CT'),
# MAGIC ('Delaware', 'DE'),
# MAGIC ('Florida', 'FL'),
# MAGIC ('Georgia', 'GA'),
# MAGIC ('Hawaii', 'HI'),
# MAGIC ('Idaho', 'ID'),
# MAGIC ('Illinois', 'IL'),
# MAGIC ('Indiana', 'IN'),
# MAGIC ('Iowa', 'IA'),
# MAGIC ('Kansas', 'KS'),
# MAGIC ('Kentucky', 'KY'),
# MAGIC ('Louisiana', 'LA'),
# MAGIC ('Maine', 'ME'),
# MAGIC ('Maryland', 'MD'),
# MAGIC ('Massachusetts', 'MA'),
# MAGIC ('Michigan', 'MI'),
# MAGIC ('Minnesota', 'MN'),
# MAGIC ('Mississippi', 'MS'),
# MAGIC ('Missouri', 'MO'),
# MAGIC ('Montana', 'MT'),
# MAGIC ('Nebraska', 'NE'),
# MAGIC ('Nevada', 'NV'),
# MAGIC ('New Hampshire', 'NH'),
# MAGIC ('New Jersey', 'NJ'),
# MAGIC ('New Mexico', 'NM'),
# MAGIC ('New York', 'NY'),
# MAGIC ('North Carolina', 'NC'),
# MAGIC ('North Dakota', 'ND'),
# MAGIC ('Ohio', 'OH'),
# MAGIC ('Oklahoma', 'OK'),
# MAGIC ('Oregon', 'OR'),
# MAGIC ('Pennsylvania', 'PA'),
# MAGIC ('Rhode Island', 'RI'),
# MAGIC ('South Carolina', 'SC'),
# MAGIC ('South Dakota', 'SD'),
# MAGIC ('Tennessee', 'TN'),
# MAGIC ('Texas', 'TX'),
# MAGIC ('Utah', 'UT'),
# MAGIC ('Vermont', 'VT'),
# MAGIC ('Virginia', 'VA'),
# MAGIC ('Washington', 'WA'),
# MAGIC ('West Virginia', 'WV'),
# MAGIC ('Wisconsin', 'WI'),
# MAGIC ('Wyoming', 'WY')
# MAGIC AS t(State, StateABV);
# MAGIC
# MAGIC -- MERGE INTO STATES TABLE
# MAGIC MERGE INTO States AS target
# MAGIC USING NewStates AS source
# MAGIC ON target.State = source.State
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (State, StateABV)
# MAGIC VALUES (source.State, source.StateABV);