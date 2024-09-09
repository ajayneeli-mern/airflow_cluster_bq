from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder \
    .appName("GCS File Read") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery_2.12:0.24.0") \
    .config("spark.sql.catalog.spark_bigquery", "com.google.cloud.spark.bigquery.BigQueryCatalog") \
    .config("spark.sql.catalog.spark_bigquery.project", "just-camera-432415-h9") \
    .config("spark.sql.catalog.spark_bigquery.dataset", "001projectbigquery") \
    .getOrCreate()

# Define the GCS path
gcs_path = "gs://001project/covid.csv"

# Read the file into a DataFrame
df = spark.read.csv(gcs_path, header=True, inferSchema=True)
select_columns =df.select("continent","country","population","`cases.new`","`cases.active`","`tests.total`")

df_clean = select_columns.withColumn("population", col("population").cast("integer"))

df_clean = select_columns.withColumn("population", col("population").cast("integer"))
df_clean=select_columns.fillna(0)
df_clean = df_clean.selectExpr(
    "continent",
    "country",
    "population",
    "`cases.new` as cases_new",
    "`cases.active` as cases_active",
    "`tests.total` as tests_total"
)
df_clean.show()

df_clean.write \
    .format("bigquery") \
    .option("table", "just-camera-432415-h9.001projectbigquery.covidworls") \
    .option("temporaryGcsBucket", "001project/temp") \
    .mode("append") \
    .save()

print('load to bigquery')