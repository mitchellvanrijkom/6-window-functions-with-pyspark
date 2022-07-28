from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Differences with lag").getOrCreate()

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

window_spec = Window.partitionBy("product").orderBy("date")

difference_df = (
    df.withColumn("previous_price", F.lag("price").over(window_spec))
    .filter(F.col("previous_price").isNotNull())
    .withColumn("difference", F.col("price") - F.col("previous_price"))
)
difference_df.show()
# +-------------------+-------------+-----+--------------+----------+
# |               date|      product|price|previous_price|difference|
# +-------------------+-------------+-----+--------------+----------+
# |2022-01-12 00:00:00|Bose Revolve+|  299|           330|       -31|
# |2022-02-10 00:00:00|Bose Revolve+|  360|           299|        61|
# |2022-02-16 00:00:00|Bose Revolve+|  330|           360|       -30|
# |2022-01-13 00:00:00| JBL Partybox|  275|           299|       -24|
# |2022-02-13 00:00:00| JBL Partybox|  269|           275|        -6|
# |2022-02-12 00:00:00|   Sonos Move|  359|           399|       -40|
# +-------------------+-------------+-----+--------------+----------+
