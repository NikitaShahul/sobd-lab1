import json

def md(text):
    return {"cell_type": "markdown", "metadata": {}, "source": text}

def code(text):
    return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": text}

SPARK_CONF = '''import os
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf


def create_spark_configuration() -> SparkConf:
    """Cozdaet i konfiguriruet ekzemplyar SparkConf dlya prilozheniya Spark."""
    user_name = os.getenv("USER")
    conf = SparkConf()
    conf.setAppName("SOBD lab1 - NYC Taxi EDA")
    conf.setMaster("yarn")
    conf.set("spark.submit.deployMode", "client")
    conf.set("spark.executor.memory", "12g")
    conf.set("spark.executor.cores", "8")
    conf.set("spark.executor.instances", "2")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.driver.cores", "2")
    conf.set("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0")
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.spark_catalog.type", "hadoop")
    conf.set("spark.sql.catalog.spark_catalog.warehouse", f"hdfs:///user/{user_name}/warehouse")
    conf.set("spark.sql.catalog.spark_catalog.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    return conf'''

DB_NAME = 'database_name = "shahulov_database"'

# ---------------- PART 1 ----------------
p1 = []
p1.append(md("## \u041b\u0430\u0431\u043e\u0440\u0430\u0442\u043e\u0440\u043d\u0430\u044f \u0440\u0430\u0431\u043e\u0442\u0430 \u2116 1\n## \u0420\u0430\u0437\u0432\u0435\u0434\u043e\u0447\u043d\u044b\u0439 \u0430\u043d\u0430\u043b\u0438\u0437 \u0431\u043e\u043b\u044c\u0448\u0438\u0445 \u0434\u0430\u043d\u043d\u044b\u0445 \u0441 Apache Spark"))
p1.append(md("### \u0427\u0430\u0441\u0442\u044c 1: \u0437\u0430\u0433\u0440\u0443\u0437\u043a\u0430, \u043e\u0447\u0438\u0441\u0442\u043a\u0430 \u0438 \u0441\u043e\u0445\u0440\u0430\u043d\u0435\u043d\u0438\u0435 \u0432 Apache Iceberg\n\n\u041d\u0430\u0431\u043e\u0440 \u0434\u0430\u043d\u043d\u044b\u0445: **NYC Yellow Taxi Trip Data** (Kaggle). \u041f\u043e\u0434\u0441\u0442\u0430\u0432\u044c\u0442\u0435 \u0441\u0432\u043e\u0439 \u043f\u0443\u0442\u044c \u0432 HDFS \u0432 \u043f\u0435\u0440\u0435\u043c\u0435\u043d\u043d\u0443\u044e path."))
p1.append(md("\u041f\u043e\u0434\u043a\u043b\u044e\u0447\u0430\u0435\u043c \u0431\u0438\u0431\u043b\u0438\u043e\u0442\u0435\u043a\u0438."))
p1.append(code(SPARK_CONF + '\n\nfrom pyspark.sql.functions import col, lit, to_timestamp, unix_timestamp'))
p1.append(md("\u0421\u043e\u0437\u0434\u0430\u0451\u043c \u043a\u043e\u043d\u0444\u0438\u0433\u0443\u0440\u0430\u0446\u0438\u044e \u0438 \u0441\u0435\u0441\u0441\u0438\u044e Spark."))
p1.append(code('conf = create_spark_configuration()'))
p1.append(code('spark = SparkSession.builder.config(conf=conf).getOrCreate()\nspark'))
p1.append(md("\u0423\u043a\u0430\u0437\u044b\u0432\u0430\u0435\u043c \u043f\u0443\u0442\u044c \u043a \u0444\u0430\u0439\u043b\u0430\u043c \u0432 HDFS (\u0437\u0430\u043c\u0435\u043d\u0438\u0442\u0435 USERNAME \u043d\u0430 \u0441\u0432\u043e\u0451 \u0438\u043c\u044f \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f)."))
p1.append(code('path = "hdfs:///user/USERNAME/datasets/nyc_taxi/*.csv"'))
p1.append(code('df = (spark.read.format("csv")\n      .option("header", "true")\n      .load(path)\n)'))
p1.append(code('df.show()'))
p1.append(code('df.printSchema()'))
p1.append(md("\u041e\u0441\u0442\u0430\u0432\u043b\u044f\u0435\u043c \u0438\u043d\u0444\u043e\u0440\u043c\u0430\u0442\u0438\u0432\u043d\u044b\u0435 \u0441\u0442\u043e\u043b\u0431\u0446\u044b."))
p1.append(code('df = df.select(\n    "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",\n    "trip_distance", "RatecodeID", "payment_type", "fare_amount",\n    "tip_amount", "tolls_amount", "total_amount", "PULocationID", "DOLocationID"\n)'))
p1.append(md("\u041f\u0440\u0438\u0432\u043e\u0434\u0438\u043c \u0441\u0442\u043e\u043b\u0431\u0446\u044b \u043a \u043a\u043e\u0440\u0440\u0435\u043a\u0442\u043d\u044b\u043c \u0442\u0438\u043f\u0430\u043c \u0438 \u0434\u043e\u0431\u0430\u0432\u043b\u044f\u0435\u043c \u0434\u043b\u0438\u0442\u0435\u043b\u044c\u043d\u043e\u0441\u0442\u044c \u043f\u043e\u0435\u0437\u0434\u043a\u0438 (\u043c\u0438\u043d)."))
p1.append(code('def transform_dataframe(data: DataFrame) -> DataFrame:\n    """Privodit stolbcy k nuzhnym tipam i dobavlyaet trip_duration_min."""\n    int_cols = ["passenger_count", "RatecodeID", "payment_type", "PULocationID", "DOLocationID"]\n    dbl_cols = ["trip_distance", "fare_amount", "tip_amount", "tolls_amount", "total_amount"]\n    for c in int_cols:\n        data = data.withColumn(c, col(c).cast("Integer"))\n    for c in dbl_cols:\n        data = data.withColumn(c, col(c).cast("Double"))\n    data = data.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))\n    data = data.withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))\n    data = data.withColumn(\n        "trip_duration_min",\n        (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60.0,\n    )\n    return data'))
p1.append(code('df = transform_dataframe(df)\ndf.printSchema()'))
p1.append(code('df.show()'))
p1.append(md("\u0421\u043e\u0445\u0440\u0430\u043d\u044f\u0435\u043c \u0432 \u0442\u0430\u0431\u043b\u0438\u0446\u0443 Apache Iceberg. \u0411\u0430\u0437\u0443 \u043d\u0430\u0437\u044b\u0432\u0430\u0435\u043c \u043f\u043e \u0444\u0430\u043c\u0438\u043b\u0438\u0438 \u0441\u0442\u0443\u0434\u0435\u043d\u0442\u0430."))
p1.append(code(DB_NAME))
p1.append(code('spark.sql(f"CREATE DATABASE IF NOT EXISTS spark_catalog.{database_name}")'))
p1.append(code('spark.catalog.setCurrentDatabase(database_name)'))
p1.append(code('df.writeTo("sobd_lab1_table").using("iceberg").create()'))
p1.append(code('for table in spark.catalog.listTables():\n    print(table.name)'))
p1.append(md("\u041f\u0440\u0438 \u043d\u0435\u043e\u0431\u0445\u043e\u0434\u0438\u043c\u043e\u0441\u0442\u0438 \u043f\u0435\u0440\u0435\u0441\u043e\u0437\u0434\u0430\u043d\u0438\u044f \u0442\u0430\u0431\u043b\u0438\u0446\u044b:"))
p1.append(code('# spark.sql("DROP TABLE spark_catalog.shahulov_database.sobd_lab1_table")\n# spark.sql("DROP DATABASE spark_catalog.shahulov_database")'))
p1.append(code('spark.stop()'))

nb1 = {"cells": p1, "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}, "language_info": {"name": "python", "version": "3.11.9"}}, "nbformat": 4, "nbformat_minor": 2}
with open("/data/1_Data_cleansing.ipynb", "w") as f:
    json.dump(nb1, f, ensure_ascii=False, indent=1)
print("part1 cells", len(p1))
