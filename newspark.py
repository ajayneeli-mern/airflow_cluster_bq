from pyspark.sql import SparkSession

def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("SimpleSparkJob") \
        .getOrCreate()

    # Create a DataFrame with some sample data
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)

    # Perform a transformation: add a new column
    df_transformed = df.withColumn("AgePlusTen", df["Age"] + 10)

    df_transformed.show()

    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
