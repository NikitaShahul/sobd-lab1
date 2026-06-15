import json

def md(text):
    return {"cell_type": "markdown", "metadata": {}, "source": text}

def code(text):
    return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": text}

SPARK_CONF = '''import os
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from pyspark.sql.functions import col, lit, sum, mean, when, count, desc, floor, corr
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


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

COUNT_NULLS = '''def count_nulls(data: DataFrame, column_name: str) -> None:
    """Podschet kolichestva null i not null znachenij v stolbce."""
    null_counts = data.select(sum(col(column_name).isNull().cast("int"))).collect()[0][0]
    not_null_counts = data.select(sum(col(column_name).isNotNull().cast("int"))).collect()[0][0]
    total = (null_counts or 0) + (not_null_counts or 0)
    pct = 100 * (null_counts or 0) / total if total else 0
    print(f"{column_name}: NULL = {null_counts} ({pct:.2f}%)")'''

PLOT_CAT = '''def plot_cat_distribution(data: DataFrame, column_name: str, top_n: int = 20) -> None:
    """Raspredelenie kategorialnogo priznaka."""
    categories = data.groupBy(column_name).count().orderBy("count", ascending=False)
    print(f"Kolichestvo kategorij {column_name}: {categories.count()}")
    categories = categories.limit(top_n).toPandas()
    plt.figure(figsize=(10, 6))
    sns.barplot(x=column_name, y="count", data=categories)
    plt.title(f"Raspredelenie \\"{column_name}\\"")
    plt.xlabel(column_name)
    plt.ylabel("Count")
    plt.xticks(rotation=45)
    plt.show()'''

PLOT_BOX = '''def plot_boxplots(data: DataFrame, columns: list, sample_fraction: float = 0.1) -> None:
    """Boxplot i statistiki dlya chislovyh stolbcov."""
    box_data = []
    for column in columns:
        q1, median, q3 = data.approxQuantile(column, [0.25, 0.5, 0.75], 0.01)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outliers_df = data.filter((col(column) < lower_bound) | (col(column) > upper_bound))
        min_value = data.agg({column: "min"}).collect()[0][0]
        mean_value = data.agg({column: "mean"}).collect()[0][0]
        std_value = data.agg({column: "std"}).collect()[0][0]
        max_value = data.agg({column: "max"}).collect()[0][0]
        lower_bound = max(lower_bound, min_value)
        upper_bound = min(upper_bound, max_value)
        outliers = []
        if not outliers_df.isEmpty():
            sampled = outliers_df.sample(sample_fraction).select(column).limit(1000).collect()
            outliers = [r[column] for r in sampled]
        box_data.append({"whislo": lower_bound, "q1": q1, "med": median, "q3": q3, "whishi": upper_bound, "fliers": outliers})
        print(f"--- {column} ---")
        print(f"min={min_value:.2f} mean={mean_value:.2f} std={std_value:.2f} q1={q1:.2f} median={median:.2f} q3={q3:.2f} max={max_value:.2f}")
    fig, ax = plt.subplots(figsize=(20, 6))
    ax.bxp(box_data, vert=False, positions=range(1, len(columns) + 1), widths=0.5)
    ax.set_yticks(range(1, len(columns) + 1))
    ax.set_yticklabels(columns)
    ax.set_xlabel("Value")
    ax.set_title("Boxplots")
    ax.grid(True)
    plt.show()'''

PLOT_QUANT = '''def plot_quant_distribution(data: DataFrame, column: str, num_bins: int = 100) -> None:
    """Gistogramma raspredeleniya chislovogo priznaka."""
    min_value = data.agg({column: "min"}).collect()[0][0]
    max_value = data.agg({column: "max"}).collect()[0][0]
    bin_size = (max_value - min_value) / num_bins
    if bin_size == 0:
        print("Stolbec postoyannyj, gistogramma ne stroitsya")
        return
    data = data.withColumn("bin", floor((col(column) - min_value) / bin_size))
    bin_counts = data.groupBy("bin").count().orderBy("bin").toPandas()
    bin_counts["bin_center"] = bin_counts["bin"].apply(lambda x: min_value + (x + 0.5) * bin_size)
    plt.figure(figsize=(20, 6))
    sns.histplot(data=bin_counts, x="bin_center", weights="count", bins=num_bins)
    plt.xlabel("Value")
    plt.ylabel("Count")
    plt.title(f"Raspredelenie \\"{column}\\"")
    plt.grid(True)
    plt.show()'''

CORR = '''def plot_correlation(data: DataFrame, columns: list) -> None:
    """Matrica korrelyacij Pirsona dlya chislovyh priznakov."""
    clean = data.select(columns).na.drop()
    assembler = VectorAssembler(inputCols=columns, outputCol="features")
    vec = assembler.transform(clean).select("features")
    matrix = Correlation.corr(vec, "features", "pearson").collect()[0][0]
    corr_pd = pd.DataFrame(matrix.toArray(), index=columns, columns=columns)
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr_pd, annot=True, fmt=".2f", cmap="coolwarm", vmin=-1, vmax=1)
    plt.title("Matrica korrelyacij")
    plt.show()
    return corr_pd'''

p2 = []
p2.append(md("## \u041b\u0430\u0431\u043e\u0440\u0430\u0442\u043e\u0440\u043d\u0430\u044f \u0440\u0430\u0431\u043e\u0442\u0430 \u2116 1\n## \u0420\u0430\u0437\u0432\u0435\u0434\u043e\u0447\u043d\u044b\u0439 \u0430\u043d\u0430\u043b\u0438\u0437 \u0431\u043e\u043b\u044c\u0448\u0438\u0445 \u0434\u0430\u043d\u043d\u044b\u0445 \u0441 Apache Spark"))
p2.append(md("### \u0427\u0430\u0441\u0442\u044c 2: \u0440\u0430\u0437\u0432\u0435\u0434\u043e\u0447\u043d\u044b\u0439 \u0430\u043d\u0430\u043b\u0438\u0437 \u0434\u0430\u043d\u043d\u044b\u0445 (EDA)"))
p2.append(code(SPARK_CONF))
p2.append(code('conf = create_spark_configuration()\nspark = SparkSession.builder.config(conf=conf).getOrCreate()\nspark'))
p2.append(code('database_name = "shahulov_database"\nspark.catalog.setCurrentDatabase(database_name)\ndf = spark.table("sobd_lab1_table")'))
p2.append(code('df.show()'))
p2.append(code('df.printSchema()'))
p2.append(code('df.count()'))
p2.append(md("#### \u041f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u043d\u044b\u0435 \u0437\u043d\u0430\u0447\u0435\u043d\u0438\u044f"))
p2.append(code(COUNT_NULLS))
p2.append(code('for c in df.columns:\n    count_nulls(df, c)'))
p2.append(md("\u0423\u0434\u0430\u043b\u044f\u0435\u043c \u044f\u0432\u043d\u044b\u0435 \u043e\u0448\u0438\u0431\u043a\u0438 \u0438 \u043f\u0440\u043e\u043f\u0443\u0441\u043a\u0438 \u0432 \u043a\u043b\u044e\u0447\u0435\u0432\u044b\u0445 \u043f\u043e\u043b\u044f\u0445."))
p2.append(code('df = df.dropna(subset=["trip_distance", "fare_amount", "total_amount", "trip_duration_min"])\ndf = df.fillna({"passenger_count": 1, "RatecodeID": 1, "payment_type": 5})\ndf.count()'))
p2.append(md("#### \u041a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u0430\u043b\u044c\u043d\u044b\u0435 \u043f\u0440\u0438\u0437\u043d\u0430\u043a\u0438"))
p2.append(code(PLOT_CAT))
p2.append(code('plot_cat_distribution(df, "payment_type")'))
p2.append(code('plot_cat_distribution(df, "passenger_count")'))
p2.append(md("#### \u0427\u0438\u0441\u043b\u043e\u0432\u044b\u0435 \u043f\u0440\u0438\u0437\u043d\u0430\u043a\u0438: \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430 \u0438 \u0432\u044b\u0431\u0440\u043e\u0441\u044b"))
p2.append(code(PLOT_BOX))
p2.append(code(PLOT_QUANT))
p2.append(code('plot_boxplots(df, ["trip_distance"])'))
p2.append(code('plot_quant_distribution(df, "trip_distance")'))
p2.append(md("\u041e\u0431\u0440\u0435\u0437\u0430\u0435\u043c \u0432\u044b\u0431\u0440\u043e\u0441\u044b \u0438 \u043d\u0435\u043a\u043e\u0440\u0440\u0435\u043a\u0442\u043d\u044b\u0435 \u0437\u043d\u0430\u0447\u0435\u043d\u0438\u044f."))
p2.append(code('df = df.filter((col("trip_distance") > 0) & (col("trip_distance") < 100))\ndf = df.filter((col("fare_amount") > 0) & (col("fare_amount") < 500))\ndf = df.filter((col("total_amount") > 0) & (col("total_amount") < 600))\ndf = df.filter((col("trip_duration_min") > 0) & (col("trip_duration_min") < 240))\ndf.count()'))
p2.append(code('plot_boxplots(df, ["trip_distance", "fare_amount", "total_amount", "trip_duration_min"])'))
p2.append(code('plot_quant_distribution(df, "fare_amount")'))
p2.append(code('plot_quant_distribution(df, "trip_duration_min")'))
p2.append(md("#### \u041a\u043e\u0440\u0440\u0435\u043b\u044f\u0446\u0438\u0438 \u043c\u0435\u0436\u0434\u0443 \u043f\u0440\u0438\u0437\u043d\u0430\u043a\u0430\u043c\u0438"))
p2.append(code(CORR))
p2.append(code('num_cols = ["trip_distance", "fare_amount", "tip_amount", "tolls_amount", "total_amount", "trip_duration_min", "passenger_count"]\ncorr_pd = plot_correlation(df, num_cols)\ncorr_pd'))
p2.append(md("#### \u0412\u044b\u0432\u043e\u0434\u044b\n\n\u0417\u0430\u043f\u043e\u043b\u043d\u0438\u0442\u0435 \u0432\u044b\u0432\u043e\u0434\u044b \u043f\u043e \u0440\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442\u0430\u043c \u0430\u043d\u0430\u043b\u0438\u0437\u0430."))
p2.append(code('spark.stop()'))

nb2 = {"cells": p2, "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}, "language_info": {"name": "python", "version": "3.11.9"}}, "nbformat": 4, "nbformat_minor": 2}
with open("/data/2_Exploratory_analysis.ipynb", "w") as f:
    json.dump(nb2, f, ensure_ascii=False, indent=1)
print("part2 cells", len(p2))
