from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import year, month, dayofmonth, asc
import os
from dotenv import load_dotenv

def read_mysql_table_to_df(sparkk, mysql_database, mysql_table_name):
    # Read mysql table to df1
    df1 = sparkk.read \
        .format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", f"jdbc:mysql://mysql:3306/{mysql_database}") \
        .option("dbtable", mysql_table_name) \
        .option("user", "root") \
        .option("password", "root") \
        .load()
    # Add columns for partitioning
    df1 = df1.withColumn("year", year("Date_Joined")) \
            .withColumn("month", month("Date_Joined")) \
            .withColumn("day", dayofmonth("Date_Joined"))
    df1 = df1.sort(["year", "month","day"], ascending=[True, True, True])
    df1.show()
    return df1

def write_dataframe_on_iceberg_table(sparkk, df1):
    # Create an iceberg database
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    df.writeTo("db.table").using("iceberg") \
        .tableProperty("write.format.default", "parquet") \
        .partitionedBy(("year"),("month"),("day")) \
        .createOrReplace()
    spark.sql("SELECT * FROM db.table").show()
    spark.sql("SELECT * FROM db.table.snapshots").show(truncate=False)

if __name__ == "__main__":
    # Load the enviroment variables
    load_dotenv()
    warehouse_path = os.getenv("warehouse_path")
    iceberg_spark_jar  = os.getenv("iceberg_spark_jar")
    catalog_name = os.getenv("catalog_name")

    mysql_host = os.getenv("mysql_host")
    mysql_user = os.getenv("mysql_user")
    mysql_password = os.getenv("mysql_password")
    mysql_database = os.getenv("mysql_database")
    mysql_table_name = os.getenv("mysql_table_name")
    # Setup iceberg config
    conf = SparkConf().setAppName("MyApp") \
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .set(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .set('spark.jars', iceberg_spark_jar) \
        .set(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path) \
        .set(f"spark.sql.catalog.{catalog_name}.type", "hadoop")\
        .set("spark.sql.defaultCatalog", catalog_name) 
    # Create spark session
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    #call functions
    df = read_mysql_table_to_df(sparkk = spark, mysql_database = mysql_database, mysql_table_name = mysql_table_name)
    write_dataframe_on_iceberg_table(sparkk = spark, df1 = df)
