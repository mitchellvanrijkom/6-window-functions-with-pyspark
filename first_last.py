from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("First Last").getOrCreate()

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

headers = ["date", "product", "price"]

data = [
    [datetime(2022, 1, 10), "Bose Revolve+", 330],
    [datetime(2022, 1, 11), "JBL Partybox", 299],
    [datetime(2022, 1, 12), "Bose Revolve+", 299],
    [datetime(2022, 1, 14), "Bose Revolve+", 399],
    [datetime(2022, 1, 18), "JBL Partybox", 300],
    [datetime(2022, 1, 29), "Bose Revolve+", 450],
    [datetime(2022, 1, 13), "JBL Partybox", 275],
    [datetime(2022, 2, 10), "Bose Revolve+", 360],
    [datetime(2022, 2, 13), "JBL Partybox", 269],
    [datetime(2022, 2, 10), "Bose Revolve+", 200],
    [datetime(2022, 2, 16), "Bose Revolve+", None],
]
df = spark.createDataFrame(data, headers)

window_spec = (
    Window.partitionBy("year", "month", "product")
    .orderBy("date")
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)

first_last_df = (
    df.withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("first_value", F.first("price", ignorenulls=True).over(window_spec))
    .withColumn("last_value", F.last("price", ignorenulls=True).over(window_spec))
    .select(["year", "month", "product", "first_value", "last_value"])
    .distinct()
)
first_last_df.show()
# +----+-----+-------------+-----------+----------+
# |year|month|      product|first_value|last_value|
# +----+-----+-------------+-----------+----------+
# |2022|    1|Bose Revolve+|        330|       450|
# |2022|    1| JBL Partybox|        299|       300|
# |2022|    2|Bose Revolve+|        360|       200|
# |2022|    2| JBL Partybox|        269|       269|
# +----+-----+-------------+-----------+----------+
