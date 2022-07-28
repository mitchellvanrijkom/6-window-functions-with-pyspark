from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("Moving Average").getOrCreate()

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

days = lambda i: i * 86400

moving_7_day_window = Window.orderBy(
    F.col("date").cast("timestamp").cast("long")
).rangeBetween(-days(7), Window.currentRow)

df = spark.createDataFrame(data, headers).withColumn(
    "mov_avg", F.avg("sales").over(moving_7_day_window)
)

df.show()
# +-------------------+-----+------------------+
# |               date|sales|           mov_avg|
# +-------------------+-----+------------------+
# |2022-01-01 00:00:00|  100|             100.0|
# |2022-01-02 00:00:00| 1543|             821.5|
# |2022-01-03 00:00:00|  756| 799.6666666666666|
# |2022-01-04 00:00:00| 2223|            1155.5|
# |2022-01-05 00:00:00|  765|            1077.4|
# |2022-01-06 00:00:00|  734|1020.1666666666666|
# |2022-01-07 00:00:00|  762| 983.2857142857143|
# |2022-01-08 00:00:00| 3422|          1288.125|
# |2022-01-09 00:00:00| 1500|          1463.125|
# |2022-01-10 00:00:00| 7332|           2186.75|
# |2022-01-11 00:00:00| 4200|           2617.25|
# |2022-01-12 00:00:00| 1121|            2479.5|
# |2022-01-13 00:00:00|  448|          2439.875|
# |2022-01-14 00:00:00| 1198|          2497.875|
# |2022-01-15 00:00:00| 1500|          2590.125|
# |2022-01-16 00:00:00| 4200|          2687.375|
# |2022-01-17 00:00:00| 1121|            2640.0|
# |2022-01-18 00:00:00|  448|            1779.5|
# |2022-01-19 00:00:00| 1198|           1404.25|
# |2022-01-20 00:00:00| 1198|          1413.875|
# +-------------------+-----+------------------+
# only showing top 20 rows

df.toPandas().plot.line(x="date", y=["sales", "mov_avg"], rot=45)
