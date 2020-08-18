#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  4 14:30:10 2020

@author: sarames
"""
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from utilFunc import *

def preprocessDataSet(rdd1):
    #rdd1_1 input => '11|:American President, The (1995)::Comedy|Drama|Romance'
    rdd1_1 = rdd1.map(lambda x: myFunc.removeComma(x))
    #rdd1_1 output = rdd2 input => '11|:American President The (1995)::Comedy|Drama|Romance'
    rdd2 = rdd1_1.flatMap(lambda x: x.split(","))
    #output => ["1:|Toy Story (1995)::Animation|Children's|Comedy",..,'11|:American President The (1995)::Comedy|Drama|Romance']
    rdd2_1 = rdd2.map(lambda x: myFunc.replaceDoubleColon(x))
    #ouput => ["1:|Toy Story (1995)|Animation|Children's|Comedy",..,'11|:American President The (1995)|Comedy|Drama|Romance']
    rdd3 = rdd2_1.map(lambda x: x.split("|"))
    #output => [['1:', 'Toy Story (1995)', 'Animation', "Children's", 'Comedy'],..,['11', ':American President The (1995)', 'Comedy', 'Drama', 'Romance']
    # Each entry has Movie_id, movie_name, movie_journals [Eg: Animation', "Children's", 'Comedy', 'Drama']
    # Aim : Identify all possible journals(=5 journals) and assign '' for missing journals
    lenRdd = rdd3.map(lambda x: len(x))
    maxLen = lenRdd.max()  # maxLen = 8, there are max 7 column => 5 journals
    # Each row should have 7 columns. If number of journals given is 3, then the remaining 7-2+3=5 columns should have empty("") val
    rdd4 = rdd3.map(lambda x: myFunc.addEmptyValToMissingCol(x, maxLen))
    # [['1:', 'Toy Story (1995)', 'Animation', "Children's", 'Comedy', '', '', ''],[, , , , ],....]
    df_rdd = rdd4.map(
        lambda x: Row(id=x[0], movie_details=x[1], journal1=x[2], journal2=x[3], journal3=x[4], journal4=x[5],
                      journal5=x[6], journal6=x[7]))
    # [Row(id='1:', journal1='Animation', journal2="Children's", journal3='Comedy', journal4='', journal5='', journal6='', movie_details='Toy Story (1995)'),..,Row(id='11', journal1='Comedy', journal2='Drama', journal3='Romance', journal4='', journal5='', journal6='', movie_details=':American President The (1995)')]
    df = spark.createDataFrame(df_rdd)  # Spark Dataframe : pyspark.sql.dataframe.DataFrame
    df.show()
    df.select('id').filter(df.journal6 != "").show()  # id: 1205 has 8 columns i.e 6 journals
    # Removes colon in id column Eg- id = 1:, 2:
    newDf = df.withColumn('id', regexp_replace('id', ':', ''))
    # Removes colon in movie_details column Eg-In id 11, movie_details=':American President The (1995)'
    newDf = newDf.withColumn('movie_details', regexp_replace('movie_details', ':', ''))
    # Convert id column from string type to Int type
    df2 = newDf.select(newDf.id.cast("int"), newDf.movie_details, newDf.journal1, newDf.journal2, newDf.journal3,
                       newDf.journal4, newDf.journal5, newDf.journal6)
    # Create Pandas Dataframe using spark df
    df_pd = df2.toPandas()
    # combine all journals columns to one column, each data separated by colon(:)
    df_pd['journals'] = df_pd['journal1'] + ':' + df_pd['journal2'] + ':' + df_pd['journal3'] + ':' + df_pd[
        'journal4'] + ':' + df_pd['journal5'] + ':' + df_pd['journal6']
    df_pd.drop(['journal1', 'journal2', 'journal3', 'journal4', 'journal5', 'journal6'], axis=1, inplace=True)
    # remove trailing :: from journals columns
    df_pd = df_pd.replace({'journals': {'::': ''}}, regex=True)
    # Creating spark dataframe from pandas dataframe
    df_spark = spark.createDataFrame(df_pd)
    return df_spark

def writeToHive(sparkDf, useParquet):
    if useParquet:
        sparkDf.write.format("parquet").save('/user/cloudera/moviesData')
        #create hive external table from the parquet file
        spark.sql("""create external table moviesTable (id bigint, movie_details string, journals string) stored as parquet location '/user/cloudera/moviesData'""");
    else:
        # Creates managed hive table moviesTable at hdfs://localhost:8020/user/hive/warehouse/moviesTable
        sparkDf.createOrReplaceTempView("mytempMoviesTable")
        sqlContext = SQLContext(spark)
        sqlContext.sql("""create table moviesTable as select * from mytempMoviesTable""")

def writeToRDBMS(useParque):
    #Approach1: useParquet=True : Exporting parquet file from hdfs to mysql using sqoop
    # sqoop export --connect "jdbc:mysql://localhost:3306/demo_1" --table moviesData1 --username "root" --password "root" --export-dir /user/cloudera/moviesData
    #Error :org.kitesdk.data.DatasetNotFoundException: Descriptor location does not exist: hdfs://localhost:8020/user/cloudera/moviesData/.metadata
    #Solution : changes to be done at source code

    #Approach2 : useParquet=False : Create mysql table directly from hive table using hcatalog
    # sqoop export --connect "jdbc:mysql://localhost:3306/demo_1" --table moviesData1 --username "root" --password "root" --hcatalog-table "moviesTable"
    #Error : Exception in thread "main" java.lang.NoClassDefFoundError: org/apache/hadoop/hive/metastore/IMetaStoreClient

    # write scoop command in python file : https://medium.com/@sandeepsinh/sqoop-jobs-development-using-python-82e4b0139457
    pass

if __name__=="__main__":
    myFunc = utilFunc()
    spark = SparkSession.builder.appName("Assign").enableHiveSupport().getOrCreate()
    # Load input movies.dat file
    moviesRdd = spark.sparkContext.textFile("file:///Dataset/movies.dat")
    sparkDf = preprocessDataSet(moviesRdd)
    writeToHive(sparkDf, useParquet=True)
    #writeToRDBMS(useParquet =True)





