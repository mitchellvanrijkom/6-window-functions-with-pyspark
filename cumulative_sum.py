from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Cumulative Sum").getOrCreate()

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import matplotlib
from datetime import datetime


headers = ["date", "sales"]

data = [
    [datetime(2022, 1, 1), 100],
    [datetime(2022, 1, 2), 1543],
    [datetime(2022, 1, 3), 756],
    [datetime(2022, 1, 4), 2223],
    [datetime(2022, 1, 5), 765],
    [datetime(2022, 1, 6), 734],
    [datetime(2022, 1, 7), 762],
    [datetime(2022, 1, 8), 3422],
    [datetime(2022, 1, 9), 1500],
    [datetime(2022, 1, 10), 7332],
    [datetime(2022, 1, 11), 4200],
    [datetime(2022, 1, 12), 1121],
    [datetime(2022, 1, 13), 448],
    [datetime(2022, 1, 14), 1198],
    [datetime(2022, 1, 15), 1500],
    [datetime(2022, 1, 16), 4200],
    [datetime(2022, 1, 17), 1121],
    [datetime(2022, 1, 18), 448],
    [datetime(2022, 1, 19), 1198],
    [datetime(2022, 1, 20), 1198],
    [datetime(2022, 1, 21), 7653],
    [datetime(2022, 1, 22), 2345],
    [datetime(2022, 1, 23), 1246],
    [datetime(2022, 1, 24), 888],
    [datetime(2022, 1, 25), 2653],
    [datetime(2022, 1, 26), 8445],
    [datetime(2022, 1, 27), 1198],
    [datetime(2022, 1, 28), 3211],
    [datetime(2022, 1, 29), 2745],
    [datetime(2022, 1, 30), 1234],
    [datetime(2022, 1, 31), 6542],
]
df = spark.createDataFrame(data, headers).withColumn(
    "cumsum", F.sum("sales").over(Window.partitionBy().orderBy("date"))
)
df.show()
# +-------------------+-----+------+
# |               date|sales|cumsum|
# +-------------------+-----+------+
# |2022-01-01 00:00:00|  100|   100|
# |2022-01-02 00:00:00| 1543|  1643|
# |2022-01-03 00:00:00|  756|  2399|
# |2022-01-04 00:00:00| 2223|  4622|
# |2022-01-05 00:00:00|  765|  5387|
# |2022-01-06 00:00:00|  734|  6121|
# |2022-01-07 00:00:00|  762|  6883|
# |2022-01-08 00:00:00| 3422| 10305|
# |2022-01-09 00:00:00| 1500| 11805|
# |2022-01-10 00:00:00| 7332| 19137|
# |2022-01-11 00:00:00| 4200| 23337|
# |2022-01-12 00:00:00| 1121| 24458|
# |2022-01-13 00:00:00|  448| 24906|
# |2022-01-14 00:00:00| 1198| 26104|
# |2022-01-15 00:00:00| 1500| 27604|
# |2022-01-16 00:00:00| 4200| 31804|
# |2022-01-17 00:00:00| 1121| 32925|
# |2022-01-18 00:00:00|  448| 33373|
# |2022-01-19 00:00:00| 1198| 34571|
# |2022-01-20 00:00:00| 1198| 35769|
# +-------------------+-----+------+
# only showing top 20 rows

df.toPandas().plot.line(x="date", y=["sales", "cumsum"], rot=45)
