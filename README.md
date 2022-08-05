# Introduction

This repository contains the source code for a blog post about window functions in PySpark. Go to [Use these 6 Window Functions to power up your PySpark queries, a comprehensive guide](https://mitchellvanrijkom.com/use-these-6-window-functions-to-power-up-your-pyspark-queries-a-comprehensive-guide). In that post, I describe how to use the 6 most window functions in PySpark.

# Window functions

Most of the time we use the SQL module in Spark. We create DataFrames with the DataFrame APIs which contain different optimizers that help support the processing of data with a wide range of data sources and algorithms for Big Data workloads.

In SQL, we have a particular type of operation called a Window Function. This operation calculates a function on a subset of rows based on the current row. For each row, a frame window is determined. On this frame, a calculation is made based on the rows in this frame. For every row, the calculation returns a value.

Because Spark uses a SQL module, we also have Window Functions at our disposal. When we combine the power of DataFrames with Window Functions, we can create some unique optimized calculations!

# Repository

## Getting started

```bash
# Create virtualenv
python -m venv .venv 

# Activate virtualenv
. .venv/bin/activate 

# Install dependencies
pip install -r requirements.txt

# Run the code
python most_recent.py
```

The repository contains the following files:

## Aggregates Functions

### How to calculate a cumulative sum (running total) 📈

Very easy with a SQL window function! 👇🏻

[cumulative_sum.py](cumulative_sum.py)

### How to calculate a moving average 📈

Filter out the noise to determine the direction of a trend!

[moving_average.py](moving_average.py)

## Ranking Functions

### Select only the most recent records

Easy way to remove duplicate entries  

[most_recent.py](most_recent.py)

### Break your dataset into equal groups

Rank each value in your dataset  

[rank.py](rank.py)

## Value/Analytical Functions

### Calculate the difference from preceeding rows

Very easy to select preceeding or following rows  

[difference.py](difference.py)

### Get the first and last value of the month

Quickly analyze the start and end of each month  

[first_last.py](first_last.py)
