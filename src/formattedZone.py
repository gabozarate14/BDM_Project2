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


def createTableIfNotExists(connection, table):
    if table.encode() not in connection.tables():
        connection.create_table(table, {'cf': dict()})
        print(f'Tabla {table} creada')


def printHBaseTable(connection, tablename):
    table = connection.table(tablename)
    for key, data in table.scan():
        print(f"Row key: {key}")
        for column, value in data.items():
            print(f"    Column: {column} => Value: {value}")


def deleteHBaseTable(connection, tablename):
    connection.delete_table(tablename, disable=True)


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


def insertToHBase(connection, table_name, rdd):
    createTableIfNotExists(connection, table_name)
    now = datetime.now().strftime("%Y%m%dT%H%M%S")
    with connection.table(table_name).batch(batch_size=1000) as batch:
        for idx, row in enumerate(rdd.collect()):
            key = "$".join([now, str(idx + 1)])
            value = json.dumps(row)
            batch.put(key, {'cf:value': value})

def dropDuplicates(rdd):
    rdd = rdd.map(lambda r: cast_lists_to_tuples(r))
    rdd = rdd.map(lambda r: tuple(r.items())).distinct().map(lambda r: dict(r))
    return rdd

def deleteDuplicatesHBase(connection):
    for table_name_b in connection.tables():
        table_name = table_name_b.decode()
        table = connection.table(table_name)
        rdd = spark.sparkContext.parallelize(list(table.scan())).map(
            lambda r: json.loads(r[1][b'cf:value'].decode())).cache()
        # Drops duplicates
        rdd = rdd.map(lambda r: cast_lists_to_tuples(r))
        rdd = rdd.map(lambda r: tuple(r.items())).distinct().map(lambda r: dict(r))
        deleteHBaseTable(connection, table_name)
        insertToHBase(connection, table_name, rdd)


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
    # Hbase tables
    idealista_table_name = config.get('hbase', 'idealista_table')
    income_table_name = config.get('hbase', 'income_table')
    price_table_name = config.get('hbase', 'price_table')
    district_table_name = config.get('hbase', 'district_table')
    neighborhood_table_name = config.get('hbase', 'neighborhood_table')

    # Connect to HBase
    connection = happybase.Connection(host=host, port=9090)
    connection.open()

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
        "dist_info_cols": [],
        "neig_col_name": "neighborhood",
        "neig_info_cols": []}

    idealista_rec = reconciliateDistNeig(idealista, lookup_di_re, lookup_ne_re, params_idealista)
    # Drops duplicates
    idealista_rec = dropDuplicates(idealista_rec)
    # Insert to hbase
    insertToHBase(connection, idealista_table_name, idealista_rec)


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
        "neig_info_cols": ['_id']}

    income_rec = reconciliateDistNeig(income, lookup_di_od, lookup_ne_od, params_income)

    # To unfold each element of the income table
    unfolded_income_rec = income_rec.flatMap(lambda row: [
        {'RFD': info["RFD"], 'pop': info["pop"], 'year': info["year"], 'idDistrict': row['idDistrict'],
         'idNeighborhood': row['idNeighborhood']} for info in row['info']])

    # Drops duplicates
    unfolded_income_rec = dropDuplicates(unfolded_income_rec)
    # Insert to hbase
    insertToHBase(connection, income_table_name, unfolded_income_rec)

    # Price

    price = spark.read.csv(price_path, header=True).rdd.map(lambda r: r.asDict()).cache()

    params_price = {
        "lu_dist_cols": ["district", "district_name", "district_reconciled"],
        "lu_neig_cols": ["neighborhood", "neighborhood_name", "neighborhood_reconciled"],
        "dist_col_name": "Nom_Districte",
        "dist_info_cols": ['Codi_Districte'],
        "neig_col_name": "Nom_Barri",
        "neig_info_cols": ['Codi_Barri']}

    price_rec = reconciliateDistNeig(price, lookup_di_od, lookup_ne_od, params_price)
    # Drops duplicates
    price_rec = dropDuplicates(price_rec)
    # Insert to hbase
    insertToHBase(connection, price_table_name, price_rec)

    # Create the Lookup tables of the database

    # District
    district = lookup_di_re.map(lambda r: (r["_id"], r["di"])).union(
        lookup_di_od.map(lambda r: (r["_id"], r["district"]))).reduceByKey(lambda a, b: a).map(
        lambda r: {"_id": r[0], "name": r[1]})
    # Drops duplicates
    district = dropDuplicates(district)
    # Insert to hbase
    insertToHBase(connection, district_table_name, district)

    # Neighbour
    neigh_x_dist = lookup_di_re.union(lookup_di_od).flatMap(
        lambda r: [(neigh, r["_id"]) for neigh in r["ne_id" if "ne_id" in r else "neighborhood_id"]]).distinct()

    neighborhood = lookup_ne_re.map(lambda r: (r["_id"], r["ne"])).union(
        lookup_ne_od.map(lambda r: (r["_id"], r["neighborhood"]))).reduceByKey(lambda a, b: a).join(neigh_x_dist).map(
        lambda r: {"_id": r[1][1], "name": r[1][0], "idDistrict": r[0]})

    # Drops duplicates
    neighborhood = dropDuplicates(neighborhood)
    # Insert to hbase
    insertToHBase(connection, neighborhood_table_name, neighborhood)

    # Delete possible duplicates generated while inserting
    deleteDuplicatesHBase(connection)

    connection.close()
