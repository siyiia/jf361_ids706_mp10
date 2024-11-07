from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os


def start_spark(appName):
    return SparkSession.builder.appName(appName).getOrCreate()


def stop_spark(spark):
    spark.stop()
    print("Spark Stopped")


def generate_report(content, title, filename="./result_report.md"):
    if not os.path.exists(filename):
        mode = "w"
    else:
        mode = "a"
    with open(filename, mode, encoding="utf-8") as file:
        file.write(f"\n## {title}\n")
        file.write(f"{content}\n")


def data_process(spark):
    df = spark.read.option("header", True).csv("dataset.txt")
    generate_report(
        df.limit(10).toPandas().to_markdown(), "Overview of Original Dataset"
    )

    # Handling missing values: Drop rows with any nulls
    df = df.dropna(how="any")
    # Type casting for specific columns (assuming 'Altitude' and 'Latitude' need to be floats)
    df = df.withColumn("Altitude", col("Altitude").cast("float"))
    df = df.withColumn("Latitude", col("Latitude").cast("float"))
    df = df.withColumn("Longitude", col("Longitude").cast("float"))

    summary_df = df.describe()
    generate_report(
        summary_df.toPandas().to_markdown(), "Descriptive Statistics of Data"
    )
    return df


def query(query_str, spark, df, table_name):
    df.createOrReplaceTempView(table_name)
    result_df = spark.sql(query_str)
    generate_report(result_df.toPandas().to_markdown(), "SQL Query Result")
    return result_df


def transform_data(df, threshold=500):
    transformed_df = df.withColumn(
        "Adjusted_Altitude", col("Altitude").cast("float") + 100.0
    )
    filter_df = transformed_df.filter(
        col("Adjusted_Altitude").cast("float") > threshold
    )
    generate_report(
        filter_df.limit(10).toPandas().to_markdown(), "Transformed Data Overview"
    )
    return filter_df


if __name__ == "__main__":
    sparkM = start_spark("Data Processing with PySpark")
    trajectory_df = data_process(sparkM)
    trajectory_table_name = "trajectory_data"
    query_string = f"""
        SELECT UserID, MAX(Altitude) AS MaxAltitude
        FROM {trajectory_table_name}
        GROUP BY UserID
        ORDER BY MaxAltitude DESC 
        """
    query(query_string, sparkM, trajectory_df, trajectory_table_name)
    transform_data(trajectory_df)
    stop_spark(sparkM)
