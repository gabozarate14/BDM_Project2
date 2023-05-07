import configparser
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import os
import sys
from datetime import datetime
import json

CONFIG_ROUTE = 'utils/config.cfg'


def addColumnExtractDate(dicc, extract_date):
    dicc["extractDate"] = extract_date
    return dicc


def readIdealista(spark, folder_path):
    idealista = None
    for filename in os.listdir(folder_path):
        subfolder = '/'.join([folder_path, filename])
        extract_date = datetime.strptime(filename[0:10], '%Y_%m_%d').date()
        if idealista:
            df = spark.read.parquet(subfolder).rdd
            df = df.map(lambda r: addColumnExtractDate(r.asDict(), extract_date))
            idealista = idealista.union(df)
        else:
            idealista = spark.read.parquet(subfolder).rdd
            idealista = idealista.map(lambda r: addColumnExtractDate(r.asDict(), extract_date))

    return idealista.cache()


def fromRowToJson(spark, lookup_path):
    lookup = spark.read.json(lookup_path).rdd
    lookup = lookup.map(lambda r: r.asDict())

    return lookup.cache()


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


def reconciliateDistNeig(main_table, lookup_district, lookup_neigh, params):
    # Read parameters
    lu_dist_cols = params["lu_dist_cols"]
    lu_neig_cols = params["lu_neig_cols"]
    dist_col_name = params["dist_col_name"]
    dist_info_cols = params["dist_info_cols"]
    neig_col_name = params["neig_col_name"]
    neig_info_cols = params["neig_info_cols"]

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

    return table_rec


def unfoldIncomeRow(row):
    id_district = row['idDistrict']
    id_neighborhood = row['idNeighborhood']
    dict_list = []
    for info_row in row['info']:
        info_dict = {
            'idDistrict': id_district,
            'idNeighborhood': id_neighborhood,
            'RFD': info_row['RFD'],
            'pop': info_row['pop'],
            'year': info_row['year']
        }
        dict_list.append(info_dict)

    return dict_list


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

    # idealista = spark.read.parquet(f"{idealista_path}/**/*").rdd.map(lambda r: r.asDict()).cache() # Reads all but does not add the date
    idealista = readIdealista(spark, idealista_path)

    # Parameters structure
    # - lu_dist_cols: column names of the lookup table that will be searched for district
    # - lu_neig_cols: column names of the lookup table that will be searched for neighborhood
    # - dist_col_name: column name that contains the district info to match in the main table (e.g. Idealista,Income, etc.)
    # - dist_info_cols: column names that contain the district infor that will be replaced by the global id
    # - neig_col_name: column name that contains the neighborhood info to match in the main table (e.g. Idealista,Income, etc.)
    # - neig_info_cols: column names that contain the neighborhood infor that will be replaced by the global id

    params_idealista = {
        "lu_dist_cols": ["di", "di_n", "di_re"],
        "lu_neig_cols": ["ne", "ne_n", "ne_re"],
        "dist_col_name": "district",
        "dist_info_cols": ['district'],
        "neig_col_name": "neighborhood",
        "neig_info_cols": ['neighborhood']}

    idealista_rec = reconciliateDistNeig(idealista, lookup_di_re, lookup_ne_re, params_idealista)
    for f in idealista_rec.collect():
        print(f)

    # To check columns that don't appear on the lookup tables
    # no_idDist = idealista_rec.filter(lambda r: r["idDistrict"] == None)
    # for f in no_idDist.collect():
    #     print(f)

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
        "dist_info_cols": ['district_id', 'district_name'],
        "neig_col_name": "neigh_name ",
        "neig_info_cols": ['_id', 'neigh_name ']}

    income_rec = reconciliateDistNeig(income, lookup_di_od, lookup_ne_od, params_income)

    # To unfold each element of the income table
    unfolded_income_rec = income_rec.flatMap(lambda row: [
        {'RFD': info["RFD"], 'pop': info["pop"], 'year': info["year"], 'idDistrict': row['idDistrict'],
         'idNeighborhood': row['idNeighborhood']} for info in row['info']])

    for f in unfolded_income_rec.collect():
        print(f)

    # Price

    price = spark.read.csv(price_path, header=True).rdd.map(lambda r: r.asDict()).cache()

    params_price = {
        "lu_dist_cols": ["district", "district_name", "district_reconciled"],
        "lu_neig_cols": ["neighborhood", "neighborhood_name", "neighborhood_reconciled"],
        "dist_col_name": "Nom_Districte",
        "dist_info_cols": ['Codi_Districte', 'Nom_Districte'],
        "neig_col_name": "Nom_Barri",
        "neig_info_cols": ['Codi_Barri', 'Nom_Barri']}

    price_rec = reconciliateDistNeig(price, lookup_di_od, lookup_ne_od, params_price)

    for f in price_rec.collect():
        print(f)

    # Create the Lookup tables of the database

    # District
    district = lookup_di_re.map(lambda r: (r["_id"], r["di"])).union(
        lookup_di_od.map(lambda r: (r["_id"], r["district"]))).reduceByKey(lambda a, b: a).map(
        lambda r: {"_id": r[0], "name": r[1]})

    for f in district.collect():
        print(f)

    # Neighbour
    neigh_x_dist = lookup_di_re.union(lookup_di_od).flatMap(
        lambda r: [(neigh, r["_id"]) for neigh in r["ne_id" if "ne_id" in r else "neighborhood_id"]]).distinct()

    neighborhood = lookup_ne_re.map(lambda r: (r["_id"], r["ne"])).union(
        lookup_ne_od.map(lambda r: (r["_id"], r["neighborhood"]))).reduceByKey(lambda a, b: a).join(neigh_x_dist).map(
        lambda r: {"_id": r[1][1], "name": r[1][0], "idDistrict": r[0]})

    for f in neighborhood.collect():
        print(f)
