# jf361_ids706_mp10

## Project Introduction
This project is to use PySpark to process data.

## Project Requirments
- Use PySpark to perform data processing on a large dataset
- Include at least one Spark SQL query and one data transformation

## Project Output
The markdown report can be found here [Result Report](./result_report.md).

## Setup
### Data Process
Read data from `dataset.txt`, then drop rows with any nulls and cast type for specific columns
```python
 df = spark.read.option("header", True).csv("dataset.txt")
 
 df = df.dropna(how="any")
 df = df.withColumn("Altitude", col("Altitude").cast("float"))
 df = df.withColumn("Latitude", col("Latitude").cast("float"))
 df = df.withColumn("Longitude", col("Longitude").cast("float"))

 summary_df = df.describe()
```

###  Spark SQL Query
Example query string is to find the maximum altitude for each person and then rank them from max to min.
```python
df.createOrReplaceTempView(table_name)
result_df = spark.sql(query_str)

example_query_str = f"""
        SELECT UserID, MAX(Altitude) AS MaxAltitude
        FROM {trajectory_table_name}
        GROUP BY UserID
        ORDER BY MaxAltitude DESC 
        """
```

### Data Transformation
This transformation adds a new column `Adjusted_Altitude` by increasing the `Altitude` by 100, and then filters the 
DataFrame to keep only rows where `Adjusted_Altitude` is greater than the specified threshold.
```python
transformed_df = df.withColumn("Adjusted_Altitude", col("Altitude").cast("float") + 100.0)
filter_df = transformed_df.filter(col("Adjusted_Altitude").cast("float") > threshold)
```