import configparser
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import os
import sys
from datetime import datetime
import json
from utils.tableSchemas import *
from pyspark.sql import Row

CONFIG_ROUTE = 'utils/config.cfg'


def addColumnExtractDate(dicc, extract_date):
    dicc["extractDate"] = extract_date
    return dicc


def cast_lists_to_tuples(d):
    for k, v in d.items():
        if isinstance(v, list):
            d[k] = tuple(v)
        elif isinstance(v, dict):
            cast_lists_to_tuples(v)
    return d


def readIdealista(spark, folder_path):
    idealista = None
    for filename in os.listdir(folder_path):
        subfolder = '/'.join([folder_path, filename])
        extract_date = datetime.strptime(filename[0:10], '%Y_%m_%d').date().strftime('%Y-%m-%d')
        if idealista:
            df = spark.read.parquet(subfolder).rdd
            df = df.map(lambda r: addColumnExtractDate(r.asDict(), extract_date))
            idealista = idealista.union(df)
        else:
            idealista = spark.read.parquet(subfolder).rdd
            idealista = idealista.map(lambda r: addColumnExtractDate(r.asDict(), extract_date))

    return idealista.cache()


def formatAfterJoin(type, listDict, delCols, neigh_col=None):
    dicc = listDict[1][0]
    for col in delCols:
        if col in dicc:
            del dicc[col]
    dicc[f"id{type}"] = listDict[1][1]
    if type == 'District':
        neigh_col = dicc[neigh_col] if neigh_col in dicc else None
        return (neigh_col, dicc)
    elif type == 'Neighborhood':
        return dicc


def reconciliateDistNeig(main_table, lookup_district, lookup_neigh, params, final_function=None):
    # Read parameters
    lu_dist_cols = params["lu_dist_cols"]
    lu_neig_cols = params["lu_neig_cols"]
    dist_col_name = params["dist_col_name"]
    dist_info_cols = params["dist_info_cols"]
    neig_col_name = params["neig_col_name"]
    neig_info_cols = params["neig_info_cols"]
    tuple_order = params["tuple_order"]

    # Reconciliate Data

    # Reconciliate District
    table_kv = main_table.map(lambda r: (r[dist_col_name] if dist_col_name in r else None, r))
    table_rec = table_kv.leftOuterJoin(
        lookup_district.flatMap(
            lambda r: [(r[i], r["_id"]) for i in lu_dist_cols]).distinct())
    table_rec = table_rec.map(lambda r: formatAfterJoin("District", r, dist_info_cols, neig_col_name))

    # Reconciliate Neighborhood
    table_rec = table_rec.leftOuterJoin(
        lookup_neigh.flatMap(lambda r: [(r[i], r["_id"]) for i in
                                        lu_neig_cols]).distinct())
    table_rec = table_rec.map(lambda r: formatAfterJoin("Neighborhood", r, neig_info_cols))

    if final_function:
        table_rec = final_function(table_rec)

    # Format to tuple
    table_rec = table_rec.map(
        lambda r: tuple((r[key] if key in r else None) for key in tuple_order)).map(
        lambda r: tuple((json.dumps(row.asDict())) if isinstance(row, Row) else row for row in r))

    return table_rec


def income_unfold(income_rec):
    # To unfold each element of the income table
    return income_rec.flatMap(lambda row: [
        {'RFD': info["RFD"], 'pop': info["pop"], 'year': info["year"], 'idDistrict': row['idDistrict'],
         'idNeighborhood': row['idNeighborhood']} for info in row['info']])

def price_fold(price_rec):
    # Fold the different prices into one row as columns
    fold_price_rec = price_rec.map(lambda x: (
        (x['Any'], x['Nom_Districte'], x['Nom_Barri'], x['idDistrict'], x['idNeighborhood']),
        [(x['Preu_mitja_habitatge'], x['Valor'])])) \
        .reduceByKey(lambda x, y: x + y)

    return fold_price_rec.map(lambda x: {
        'Any': x[0][0],
        'Nom_Districte': x[0][1],
        'Nom_Barri': x[0][2],
        'idDistrict': x[0][3],
        'idNeighborhood': x[0][4],
        **dict(x[1])
    })

if __name__ == '__main__':
    # Get the parameters
    config = configparser.ConfigParser()
    config.read(CONFIG_ROUTE)
    # Server info
    host = config.get('data_server', 'host')
    user = config.get('data_server', 'user')
    # Spark
    pyspark_python = config.get('pyspark', 'python')
    pyspark_driver_python = config.get('pyspark', 'driver_python')
    # Hadoop
    hadoop_home = config.get('hadoop', 'home')
    # Paths
    lookup_path = config.get('routes', 'lookup_tables')
    idealista_path = config.get('routes', 'idealista')
    income_path = config.get('routes', 'income_opendata')
    price_path = config.get('routes', 'opendatabcn-price')

    # Set Spark
    os.environ["HADOOP_HOME"] = hadoop_home
    sys.path.append(hadoop_home + "\\bin")
    os.environ["PYSPARK_PYTHON"] = pyspark_python
    os.environ["PYSPARK_DRIVER_PYTHON"] = pyspark_driver_python

    # Set Spark
    conf = SparkConf() \
        .set("spark.master", "local") \
        .set("spark.app.name", "Formatted Zone")

    # Create the session
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    # Data Reconciliation

    # Idealista with Rent Lookup Tables

    lookup_di_re = spark.read.json(f"{lookup_path}/rent_lookup_district.json").rdd.map(lambda r: r.asDict()).cache()
    lookup_ne_re = spark.read.json(f"{lookup_path}/rent_lookup_neighborhood.json").rdd.map(lambda r: r.asDict()).cache()

    idealista = readIdealista(spark, idealista_path)

    # Parameters structure
    # - lu_dist_cols: column names of the lookup table that will be searched for district
    # - lu_neig_cols: column names of the lookup table that will be searched for neighborhood
    # - dist_col_name: column name that contains the district info to match in the main table (e.g. Idealista,Income, etc.)
    # - dist_info_cols: column names that contain the district infor that will be replaced by the global id
    # - neig_col_name: column name that contains the neighborhood info to match in the main table (e.g. Idealista,Income, etc.)
    # - neig_info_cols: column names that contain the neighborhood infor that will be replaced by the global id
    # - tuple_order: schema definition

    params_idealista = {
        "lu_dist_cols": ["di", "di_n", "di_re"],
        "lu_neig_cols": ["ne", "ne_n", "ne_re"],
        "dist_col_name": "district",
        "dist_info_cols": [],
        "neig_col_name": "neighborhood",
        "neig_info_cols": [],
        "tuple_order": IDEALISTA_SCHEMA}

    idealista_rec = reconciliateDistNeig(idealista, lookup_di_re, lookup_ne_re, params_idealista, None)

    # Drops duplicates
    idealista_rec = idealista_rec.distinct()

    idealista_hdfs_path = config.get('hdfs_formatted', 'idealista')
    # output_directory = "../output/idealista"
    output_directory = f"hdfs://{host}:27000/{idealista_hdfs_path}"
    idealista_df = spark.createDataFrame(idealista_rec,
                                         IDEALISTA_SCHEMA)

    idealista_df.write.mode("overwrite").parquet(output_directory)

    # Reconciliate date with OpenData Lookup Tables

    lookup_di_od = spark.read.json(f"{lookup_path}/income_lookup_district.json").rdd.map(lambda r: r.asDict()).cache()
    lookup_ne_od = spark.read.json(f"{lookup_path}/income_lookup_neighborhood.json").rdd.map(
        lambda r: r.asDict()).cache()

    # Income x Neighborhood

    income = spark.read.json(income_path).rdd.map(lambda r: r.asDict()).cache()

    params_income = {
        "lu_dist_cols": ["district", "district_name", "district_reconciled"],
        "lu_neig_cols": ["neighborhood", "neighborhood_name", "neighborhood_reconciled"],
        "dist_col_name": "district_name",
        "dist_info_cols": ['district_id'],
        "neig_col_name": "neigh_name ",
        "neig_info_cols": ['_id'],
        "tuple_order" : INCOME_SCHEMA
    }

    income_rec = reconciliateDistNeig(income, lookup_di_od, lookup_ne_od, params_income, income_unfold)

    # Drops duplicates
    idealista_rec = income_rec.distinct()

    income_hdfs_path = config.get('hdfs_formatted', 'income')
    # output_directory = "../output/income"
    output_directory = f"hdfs://{host}:27000/{income_hdfs_path}"
    income_df = spark.createDataFrame(income_rec,
                                      INCOME_SCHEMA)

    income_df.write.mode("overwrite").parquet(output_directory)

    # Price

    price = spark.read.csv(price_path, header=True).rdd.map(lambda r: r.asDict()).cache()

    params_price = {
        "lu_dist_cols": ["district", "district_name", "district_reconciled"],
        "lu_neig_cols": ["neighborhood", "neighborhood_name", "neighborhood_reconciled"],
        "dist_col_name": "Nom_Districte",
        "dist_info_cols": ['Codi_Districte'],
        "neig_col_name": "Nom_Barri",
        "neig_info_cols": ['Codi_Barri'],
        "tuple_order": PRICE_SCHEMA,
    }

    price_rec = reconciliateDistNeig(price, lookup_di_od, lookup_ne_od, params_price, price_fold)

    # Drops duplicates
    price_rec = price_rec.distinct()

    price_hdfs_path = config.get('hdfs_formatted', 'price')
    # output_directory = "../output/price"
    output_directory = f"hdfs://{host}:27000/{price_hdfs_path}"
    price_df = spark.createDataFrame(price_rec,
                                      PRICE_SCHEMA)

    price_df.write.mode("overwrite").parquet(output_directory)


    # Create the Lookup tables of the database

    # District
    district = lookup_di_re.map(lambda r: (r["_id"], r["di"])).union(
        lookup_di_od.map(lambda r: (r["_id"], r["district"]))).reduceByKey(lambda a, b: a).map(
        lambda r: (r[0], r[1]))
    # Drops duplicates
    district = district.distinct()

    district_hdfs_path = config.get('hdfs_formatted', 'district')
    # output_directory = "../output/district"
    output_directory = f"hdfs://{host}:27000/{district_hdfs_path}"
    district_df = spark.createDataFrame(district,
                                      DISTRICT_SCHEMA)

    district_df.write.mode("overwrite").parquet(output_directory)

    # Neighbour
    neigh_x_dist = lookup_di_re.union(lookup_di_od).flatMap(
        lambda r: [(neigh, r["_id"]) for neigh in r["ne_id" if "ne_id" in r else "neighborhood_id"]]).distinct()

    neighborhood = lookup_ne_re.map(lambda r: (r["_id"], r["ne"])).union(
        lookup_ne_od.map(lambda r: (r["_id"], r["neighborhood"]))).reduceByKey(lambda a, b: a).join(neigh_x_dist).map(
        lambda r: (r[0], r[1][0], r[1][1]))

    # Drops duplicates
    neighborhood = neighborhood.distinct()

    neighborhood_hdfs_path = config.get('hdfs_formatted', 'neighborhood')
    # output_directory = "../output/neighborhood"
    output_directory = f"hdfs://{host}:27000/{neighborhood_hdfs_path}"
    neighborhood_df = spark.createDataFrame(neighborhood,
                                        NEIGHBORHOOD_SCHEMA)

    neighborhood_df.write.mode("overwrite").parquet(output_directory)

    spark.stop()
