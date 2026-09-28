# Viva Questions and Answers — Big Data Analytics (Apache Spark Project)

Simple, student-friendly answers you can explain confidently in a viva.

---

**1. What is Big Data?**
Big Data refers to datasets that are too large, fast-changing, or varied in
structure to be processed efficiently by traditional single-machine tools.
It is often described using the "5 Vs": Volume, Velocity, Variety, Veracity,
and Value.

**2. Why did you use Apache Spark for this project?**
Spark processes data in-memory and in a distributed manner, making it much
faster than traditional disk-based tools like Hadoop MapReduce. It also
provides a simple, high-level DataFrame API and SQL interface, making it
ideal for both quick analytics and large-scale processing.

**3. What is the difference between Spark and Hadoop MapReduce?**
Hadoop MapReduce writes intermediate results to disk between each stage,
making it slower. Spark keeps data in memory (RAM) between operations,
making iterative and interactive analytics much faster — often 10-100x
faster for suitable workloads.

**4. What is PySpark?**
PySpark is the Python API for Apache Spark. It lets Python developers write
Spark applications using familiar Python syntax while still benefiting from
Spark's distributed processing engine (which is written in Scala/JVM).

**5. What is a Spark DataFrame?**
A DataFrame is a distributed collection of data organized into named
columns, conceptually similar to a table in a relational database or a
Pandas DataFrame — but it can be partitioned and processed across multiple
machines/cores.

**6. What is an RDD?**
RDD stands for Resilient Distributed Dataset. It is Spark's original
low-level data structure — an immutable, distributed collection of objects
that can be processed in parallel. DataFrames are built on top of RDDs but
offer a higher-level, optimized API.

**7. What is Spark SQL?**
Spark SQL is a Spark module that lets you run SQL queries directly against
DataFrames by registering them as temporary views. In this project,
`src/spark_sql_queries.py` demonstrates this with 7 SQL queries.

**8. What is lazy evaluation in Spark?**
Spark does not execute transformations (like `filter` or `groupBy`)
immediately. Instead, it builds up a logical execution plan (a DAG) and
only executes it when an action (like `count()` or `show()`) is called.
This allows Spark to optimize the entire chain of operations before running it.

**9. What are Transformations in Spark?**
Transformations are operations that create a new DataFrame from an existing
one, such as `filter()`, `select()`, `groupBy()`, and `withColumn()`. They
are lazy — they don't execute immediately.

**10. What are Actions in Spark?**
Actions trigger actual computation and return a result, such as `count()`,
`collect()`, `show()`, and `toPandas()`. In this project, `.count()` and
`.toPandas()` are used as actions to materialize results.

**11. Give an example of a transformation and an action used in this project.**
`df.groupBy("product_category").agg(F.sum("total_amount"))` is a
transformation. Calling `.toPandas()` on the result is an action that
triggers the actual computation.

**12. What is GroupBy in Spark?**
`groupBy()` groups rows that share the same value in one or more columns so
that aggregate functions (sum, avg, count) can be applied to each group —
for example, grouping transactions by `product_category` to compute
category-wise revenue.

**13. What is Aggregation in Spark?**
Aggregation combines multiple rows into a summary value using functions like
`sum()`, `avg()`, `count()`, `min()`, and `max()`. This project uses
aggregation extensively in `src/analytics.py`, e.g., to compute total
revenue and average order value.

**14. What is Partitioning in Spark?**
Partitioning is how Spark splits a dataset into smaller chunks so that
different chunks can be processed in parallel across CPU cores or cluster
nodes. This project sets `spark.sql.shuffle.partitions` to a small value
since the dataset is small.

**15. What is Fault Tolerance in Spark?**
Spark achieves fault tolerance by tracking the lineage (sequence of
transformations) used to build each RDD/DataFrame. If a partition of data
is lost due to a node failure, Spark can recompute just that partition using
its lineage, instead of restarting the whole job.

**16. What is a Spark Driver?**
The Driver is the main process that runs the user's application code,
creates the SparkSession, and coordinates the execution of tasks across
Executors. In this project, the Driver runs on your local machine.

**17. What is a Spark Executor?**
Executors are worker processes that actually run the tasks assigned by the
Driver and store data in memory/disk for the application. In local mode
(`local[*]`), Executors run as threads on your own machine.

**18. What is a Spark Cluster?**
A cluster is a group of machines working together to run a Spark
application, typically managed by a cluster manager (e.g. YARN,
Kubernetes, or Spark's Standalone manager). This project intentionally
uses `local[*]` mode (a "cluster of one machine") so it can run without
any cluster setup.

**19. What does `local[*]` mean in `SparkSession.builder.master("local[*]")`?**
It tells Spark to run in local mode using all available CPU cores on the
current machine as if they were Spark executors, rather than connecting to
a real distributed cluster.

**20. What is ETL?**
ETL stands for Extract, Transform, Load — the standard process of pulling
raw data from a source (Extract), cleaning/reshaping it (Transform), and
storing/serving the processed result (Load). This project's pipeline
(`run_pipeline.py`) follows exactly this pattern.

**21. Why is Data Cleaning important in Big Data Analytics?**
Raw data often contains missing values, duplicates, and invalid entries.
If left unhandled, these issues distort aggregate results (e.g., inflating
revenue with duplicate transactions). Cleaning ensures analytics reflect
accurate, trustworthy data.

**22. How did you handle missing values in this project?**
Categorical columns (gender, city, payment method) are filled with
"Unknown" so the row isn't lost. `customer_age` is imputed with the median
age. Rows missing `unit_price` are dropped since price is essential for
revenue calculations. This logic is in `src/data_cleaning.py`.

**23. How did you handle duplicate records?**
`dropDuplicates(["transaction_id"])` is used to remove exact duplicate
transactions based on the unique transaction ID, ensuring each sale is
counted only once.

**24. How did you handle invalid dates?**
`to_date()` is used to parse the date string using the expected format
(`yyyy-MM-dd`). If a date is malformed (e.g. `"31-02-2023"`), parsing
returns `null`, and such rows are filtered out since they cannot be placed
correctly in time-based analyses.

**25. What is the `total_amount` column and how is it calculated?**
`total_amount` is a derived (calculated) column representing the revenue
from a single transaction line, computed as
`total_amount = quantity * unit_price` in `src/data_processing.py`.

**26. Why did you use Streamlit for the dashboard?**
Streamlit lets you build an interactive web dashboard using pure Python
(no HTML/CSS/JavaScript required), with built-in widgets like sliders,
multiselects, and metrics — making it fast to build and easy for a
non-web-developer to understand and demo.

**27. Why did the dashboard use Pandas instead of Spark directly?**
The Spark pipeline (`run_pipeline.py`) does the heavy distributed
processing once and saves the small, already-aggregated result
(~10,000 rows) to `output/cleaned_data.csv`. The dashboard then uses Pandas
to filter this small dataset instantly. Re-running a full Spark job on
every filter click would be unnecessarily slow for a dataset this size —
this mirrors how real BI dashboards sit on top of a Spark batch pipeline.

**28. What is Data Visualization and why is it important?**
Data visualization is the graphical representation of data (charts, graphs)
that makes patterns, trends, and outliers easier to understand than raw
numbers or tables. This project uses Plotly to visualize revenue trends,
category breakdowns, and customer demographics.

**29. What is the difference between the DataFrame API and Spark SQL in this project?**
Both do the same underlying computation — `src/analytics.py` uses the
DataFrame API (`groupBy().agg()`) while `src/spark_sql_queries.py` uses
plain SQL strings executed via `spark.sql()`. Spark SQL is often more
readable for people with a SQL background, while the DataFrame API is more
composable in Python code.

**30. What does `.cache()` do in Spark, and why did you use it?**
`.cache()` tells Spark to keep a DataFrame in memory after it is first
computed, so that subsequent actions on it don't have to recompute it from
scratch. In `run_pipeline.py`, `processed_df.cache()` is used because the
same processed DataFrame is reused across many different aggregations and
SQL queries.

**31. What is schema-on-read vs schema-on-write?**
Schema-on-write (traditional databases) requires defining the structure
before loading data. Schema-on-read (used by Spark/Big Data tools) allows
loading raw data first and applying structure (a schema) at read time —
this project explicitly defines a schema in `data_ingestion.py` when
reading the CSV.

**32. How would you scale this project to a real multi-node cluster?**
The same PySpark code would work largely unchanged — you would simply
change `.master("local[*]")` to point to a cluster manager (e.g. YARN or a
Spark Standalone cluster URL) and place the input data on a distributed
file system like HDFS or S3 instead of a local CSV.

**33. What is the difference between `.count()` and `.show()` as actions?**
`.count()` returns the number of rows as an integer (triggers computation
but no display). `.show()` triggers computation and prints a formatted
preview of the DataFrame's rows to the console.

**34. Why did you convert some Spark DataFrames to Pandas using `.toPandas()`?**
`.toPandas()` collects the (already small, aggregated) result from Spark's
distributed workers back into a single Pandas DataFrame on the driver, which
is then easy to save as CSV or hand to Streamlit/Plotly for visualization.
It should only be used on small, aggregated results — not on huge raw
datasets — which is exactly how it's used in this project.

**35. What are some real-world use cases of Big Data Analytics in e-commerce?**
Examples include personalized product recommendations, dynamic pricing,
fraud detection, customer segmentation, demand forecasting, and inventory
optimization — many of which build on the same kind of aggregated
transaction analytics demonstrated in this project.
