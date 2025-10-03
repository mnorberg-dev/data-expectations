
# Data Quality with Delta Live Tables (Lakeflow Declarative Pipelines)

This repository contains the **slides and demo code** for the talk *"Implementing Data Quality: A Practical Guide on Data Quality Expectations with Delta Live Tables"*.  

Although Databricks has since rebranded **Delta Live Tables (DLT)** as **Lakeflow Declarative Pipelines**, the concepts and strategies shown here are still directly applicable.  

---

## Contents

- `slides/`  
    PDF or PowerPoint version of the presentation slides.  

- `Demo_Data_Creation_Script.py`  

    Python script used to create a synthetic dataset with deliberate data quality issues for demonstration purposes.  

- `DLT_Demo_SQL.sql`  

    Example showing to apply data quality expectations in SQL.

    ```sql
    CREATE OR REFRESH MATERIALIZED VIEW Terrible_Trips_DQ_SQL(
    CONSTRAINT distance_example EXPECT (trip_distance > 0)
    )
    AS SELECT * FROM `Your-Catalog`.dais.terrible_trips
    ```

- `DLT_Demo.py`

    Python script demonstrating how to define data expectations programmatically.

    > **Note:** Some code in this file is intentionally commented out. These variations were used to create different examples for the slides and demo screenshots. If you watch the presentation video, you’ll see how each piece fits together, and you can decide which tables or expectations you want to enable in your own environment.  

- `Data_Quality_Examples.sql`

    SQL queries used to create screenshots shown in presentation.

    > **Note:** One of the examples requires you to get the pipeline id from your infrastructure.

## License

MIT License – feel free to use and adapt the code for your own projects.