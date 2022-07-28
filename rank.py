from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ranking values").getOrCreate()

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

df.withColumn(
    "group", F.ntile(3).over(Window.partitionBy("product").orderBy("price"))
).show()
# +-------------------+-------------+-----+-----+
# |               date|      product|price|group|
# +-------------------+-------------+-----+-----+
# |2022-01-12 00:00:00|Bose Revolve+|  299|    1|
# |2022-01-10 00:00:00|Bose Revolve+|  330|    1|
# |2022-02-16 00:00:00|Bose Revolve+|  330|    2|
# |2022-02-10 00:00:00|Bose Revolve+|  360|    3|
# |2022-02-13 00:00:00| JBL Partybox|  269|    1|
# |2022-01-13 00:00:00| JBL Partybox|  275|    2|
# |2022-01-11 00:00:00| JBL Partybox|  299|    3|
# |2022-02-12 00:00:00|   Sonos Move|  359|    1|
# |2022-01-12 00:00:00|   Sonos Move|  399|    2|
# +-------------------+-------------+-----+-----+
