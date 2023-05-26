import configparser
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import os
import sys
from datetime import datetime
import json
import happybase

CONFIG_ROUTE = 'utils/config.cfg'
MODEL_ROUTE = '../model/'


def readFormatted(table):
    hdfs_path = config.get('hdfs_formatted', table)
    directory = f"hdfs://{host}:27000/{hdfs_path}"
    df = spark.read.parquet(directory)
    return df.rdd.map(lambda r: r.asDict()).cache()


def formatPredKPI1(listDict):
    dict1 = listDict[0]
    dict2 = listDict[1]
    # KPI = RFD / Total. Euros/m2 construït
    try:
        kpi = round(dict1['RFD'] / float(dict2['Total. Euros/m2 construït']), 4)
    except:
        kpi = 0

    ans = (
        dict1['year'],
        dict1['idDistrict'],
        dict1['idNeighborhood'],
        dict1['RFD'],
        dict1['pop'],
        dict2['Nou. Euros/m2 construït'],
        dict2["Nou. Milers d'euros"],
        dict2['Total. Euros/m2 construït'],
        dict2["Total. Milers d'euros"],
        dict2['Usat. Euros/m2 construït'],
        dict2["Usat. Milers d'euros"],
        kpi
    )

    return ans


def extract_tuples(tuple_with_tuples):
    extracted_tuple = tuple()
    for element in tuple_with_tuples:
        if isinstance(element, tuple):
            extracted_tuple += extract_tuples(element)
        else:
            extracted_tuple += (element,)
    return extracted_tuple


if __name__ == '__main__':
    # Get the parameters
    config = configparser.ConfigParser()
    config.read(CONFIG_ROUTE)
    # Server info
    host = config.get('data_server', 'host')
    # Spark
    pyspark_python = config.get('pyspark', 'python')
    pyspark_driver_python = config.get('pyspark', 'driver_python')
    # Hadoop
    hadoop_home = config.get('hadoop', 'home')

    # HDFS
    host = config.get('data_server', 'host')
    user = config.get('data_server', 'user')
    hdfs_path = config.get('routes', 'hdfs')

    # Set Spark
    os.environ["HADOOP_HOME"] = hadoop_home
    sys.path.append(hadoop_home + "\\bin")
    os.environ["PYSPARK_PYTHON"] = pyspark_python
    os.environ["PYSPARK_DRIVER_PYTHON"] = pyspark_driver_python

    # Set Spark
    conf = SparkConf() \
        .set("spark.master", "local") \
        .set("spark.app.name", "Exploitation Zone - Dashboard") \
        .set("spark.executor.memory", "8g") \
        .set("spark.driver.memory", "4g") \
        .set("spark.sql.shuffle.partitions", "4") \
        .set("spark.default.parallelism", "8") \
        .set("spark.jars", "resources/postgresql-jdbc.jar")

    # Create the session
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    # Postgres Connection settings
    jdbc_url = config.get('postgres_dashboard', 'url')
    connection_properties = {
        "user": config.get('postgres_dashboard', 'user'),
        "password": config.get('postgres_dashboard', 'password'),
        "driver": "org.postgresql.Driver"
    }

    income_rdd = readFormatted('income')
    price_rdd = readFormatted('price')
    idealista_rdd = readFormatted('idealista')
    neighborhood_rdd = readFormatted('neighborhood')
    district_rdd = readFormatted('district')

    # Collect the RDD to accelerate the calculations
    income_rdd.collect()
    price_rdd.collect()
    idealista_rdd.collect()
    neighborhood_rdd.collect()
    district_rdd.collect()

    dist_join = district_rdd.map(lambda r: (r['_id'], r['name'])).cache()
    neigh_join = neighborhood_rdd.map(lambda r: (r['_id'], r['name'])).cache()

    # KPI 1: ratio_RFD_m2_built
    ratio_RFD_m2_built_rdd = income_rdd.filter(lambda r: r['idNeighborhood'] and r['idNeighborhood']).map(lambda r: (
        (str(r['year']) if r['year'] else '') + (r['idNeighborhood'] if r['idNeighborhood'] else ''), r)).join(
        price_rdd.map(
            lambda r: (
                (str(r['Any']) if r['Any'] else '') + (r['idNeighborhood'] if r['idNeighborhood'] else ''), r))).map(
        lambda r: formatPredKPI1(r[1])).map(lambda r: (r[1], r)).join(dist_join).map(
        lambda r: (r[1][0][2], r)).join(neigh_join).map(lambda r: extract_tuples(r)).map(
        lambda r: tuple(r[i] for i in (2, 14, 15, 5, 6, 7, 8, 9, 10, 11, 12, 13))).map(
        lambda r: tuple(v if v != 'NA' else None for v in r))

    df_ratio_RFD_m2_built_rdd = spark.createDataFrame(ratio_RFD_m2_built_rdd,
                                                      ['Year',
                                                       'District',
                                                       'Neighborhood',
                                                       'RFD',
                                                       'Population',
                                                       'New_Euros_m2_built',
                                                       "New_Thousand_of_Euros",
                                                       'Total_Euros_m2_built',
                                                       "Total_Thousand_of_Euros",
                                                       'Used_Euros_m2_built',
                                                       "Used_Thousand_of_Euros",
                                                       'Kpi'])

    df_ratio_RFD_m2_built_rdd.write. \
        option("reWriteBatchedInserts", "true") \
        .jdbc(jdbc_url, table="ratio_RFD_m2_built", mode="overwrite", properties=connection_properties)

    # KPI 2: Average income x year
    avg_income_x_year = income_rdd.filter(lambda x: x["idDistrict"] and x["idNeighborhood"]).map(
        lambda x: ((x["year"], x["idDistrict"], x["idNeighborhood"]), (x["RFD"], 1))).reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1])).map(
        lambda x: extract_tuples((x[0], x[1][0] / x[1][1]))).map(lambda r: (r[1], r)).join(dist_join).map(
        lambda r: (r[1][0][2], r)).join(neigh_join).map(lambda r: extract_tuples(r)).map(
        lambda r: (str(r[2]), r[6], r[7], str(r[5])))

    df_avg_income_x_year = spark.createDataFrame(avg_income_x_year,
                                                 ["Year", "District", "Neighborhood", "RFD"])

    df_avg_income_x_year.write.option("reWriteBatchedInserts", "true").jdbc(jdbc_url, table="avg_income_x_year",
                                                                            mode="overwrite",
                                                                            properties=connection_properties)

    # KPI 3. Average flat sale price x month
    avg_flat_sale_price_x_month = idealista_rdd.filter(
        lambda r: r['propertyType'] == 'flat' and r['operation'] == 'sale' and r["idDistrict"] and
                  r["idNeighborhood"]).map(
        lambda r: (
            (r['extractDate'][0:4], r['extractDate'][5:7], r['idDistrict'], r['idNeighborhood']),
            (r['price'], 1))).reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1])).map(
        lambda x: extract_tuples((x[0], x[1][0] / x[1][1]))).map(lambda r: (r[2], r)).join(dist_join).map(
        lambda r: (r[1][0][3], r)).join(neigh_join).map(lambda r: extract_tuples(r)).map(
        lambda r: (str(r[2]), str(r[3]), r[7], r[8], str(r[6])))

    df_avg_flat_sale_price_x_month = spark.createDataFrame(avg_flat_sale_price_x_month,
                                                           ["Year", "Month", "District", "Neighborhood", "Price"])
    df_avg_flat_sale_price_x_month.write. \
        option("reWriteBatchedInserts", "true") \
        .jdbc(jdbc_url, table="avg_flat_sale_price_x_month", mode="overwrite", properties=connection_properties)

    spark.stop()
