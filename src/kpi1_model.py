import configparser
import pyspark
from pyspark import SparkConf
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, DoubleType
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
import random

CONFIG_ROUTE = 'utils/config.cfg'
MODEL_ROUTE = '../model/kpi1_linear_regression'

if __name__ == '__main__':
    random.seed(10)
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
        .set("spark.app.name", "KPI1 Model")

    # Create the session
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    data_path = f"hdfs://{host}:27000/{hdfs_path}/model/kpi1"
    # data_path = '../output/kpi1'

    df = spark.read.csv(data_path, header=True, inferSchema=True)

    # Set correct datatypes
    column_types = {'Year': IntegerType(),
                    'District': StringType(),
                    'Neighborhood': StringType(),
                    'RFD': DoubleType(),
                    'Population': IntegerType(),
                    'New Euros/m2 built': DoubleType(),
                    "New Thousand of Euros": DoubleType(),
                    'Total Euros/m2 built': DoubleType(),
                    "Total Thousand of Euros": DoubleType(),
                    'Used Euros/m2 built': DoubleType(),
                    "Used Thousand of Euros": DoubleType(),
                    }

    # Set column types using withColumn() and cast()
    for col_name, col_type in column_types.items():
        df = df.withColumn(col_name, df[col_name].cast(col_type))

    # Data cleaning
    df_clean = df.dropna()

    # Select the relevant columns for your features and target variable
    # categorical_columns = ['District', 'Neighborhood']
    feature_columns = ['Year', 'RFD', 'Population', 'New Euros/m2 built', "New Thousand of Euros",
                         'Total Euros/m2 built', "Total Thousand of Euros", 'Used Euros/m2 built',
                         "Used Thousand of Euros"]
    target_column = "Kpi"

    df_clean.show(10)

    # Create a vector assembler to combine the feature columns into a single "features" column
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

    # Transform the dataset to include the "features" column
    dataset = assembler.transform(df_clean)

    # Split the dataset into training and testing sets
    (training_data, testing_data) = dataset.randomSplit([0.8, 0.2])

    # Create a Linear Regression model
    lr = LinearRegression(featuresCol="features", labelCol=target_column)

    # Fit the model to the training data
    model = lr.fit(training_data)

    # Make predictions on the testing data
    predictions = model.transform(testing_data)

    # Evaluate the model using a regression evaluator
    evaluator = RegressionEvaluator(labelCol=target_column, metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})

    # Print evaluation metrics
    print("Root Mean Squared Error (RMSE):", rmse)
    print("R-squared (R2):", r2)

    # Print the model coefficients and intercept
    print("Coefficients:", model.coefficients)
    print("Intercept:", model.intercept)

    # Save the model
    model.save(MODEL_ROUTE)

    # Stop the Spark session
    spark.stop()
