from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Most Recent Record").getOrCreate()

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

headers = ["date", "product", "price"]

data = [
    [datetime(2022, 1, 10), "Bose Revolve+", 330],
    [datetime(2022, 1, 11), "JBL Partybox", 299],
    [datetime(2022, 1, 12), "Bose Revolve+", 299],
    [datetime(2022, 1, 12), "Sonos Move", 399],
    [datetime(2022, 1, 13), "JBL Partybox", 275],
    [datetime(2022, 2, 10), "Bose Revolve+", 360],
    [datetime(2022, 2, 12), "Sonos Move", 359],
    [datetime(2022, 2, 13), "JBL Partybox", 269],
    [datetime(2022, 2, 16), "Bose Revolve+", 330],
]
df = spark.createDataFrame(data, headers)


product_window = Window.partitionBy("product").orderBy(F.col("date").desc())

recent_df = (
    df.withColumn("row_num", F.row_number().over(product_window))
    # We now have created a subsequent numbering based on date descending for every product
    # Simply filter the rows with row_num = 1 to select the most recent record
    .filter(F.col("row_num") == 1)
    # # We do not need the row_number column anymore, so we drop it
    .drop("row_num")
)
recent_df.show()
# +-------------------+-------------+-----+
# |               date|      product|price|
# +-------------------+-------------+-----+
# |2022-02-16 00:00:00|Bose Revolve+|  330|
# |2022-02-13 00:00:00| JBL Partybox|  269|
# |2022-02-12 00:00:00|   Sonos Move|  359|
# +-------------------+-------------+-----+
