# UnstructuredData_Spark
Cleaning unstructured data(.dat) using SPARK RDD,creating Dataframe, storing the results in HIVE 

### Dataset
Movies dataset stored in .dat file - movies.dat

### Project Goal

#### Unstructured Data -> HDFS  -> HIVE External table -> RDBMS

1. Given unstructured input dataset. 
2. Clean the dataset using RDD and create spark dataframe. Apply necessary transformations
3. Store the contents of spark dataframe to file in parquet format or create temptable using spark dataframe
4. Create HIVE(managed/external) table from parquet file or directly from the temptable
5. Using SQOOP , export the data from Hive-table/HDFS-parquet-file to RDBMS table  - ISSUE: exporting parquet file using SQOOP throws error

### Files used in the project
1. main.py : This file has the code for cleaning the dataset using RDD,creating Spark Dataframe, storing data in HDFS,creating HIVE table (and sqoop                    export command)
2. utilFunc.py : This is the supporting file which has functions for data cleaning, which are used in the project.py 
3. submitCommand.txt:  This file has the spark-submit command

### Steps to execute
1. Start hive server (hive --service metastore &)
2. Modify the paths of movie.dat,parquet file location in the main.py
3. Download main.py, utilFunc.py and submitCommand.txt
4. In the VM where Hadoop environment is setup, execute the spark-submit command present in submitCommand.txt
5. In the hive CLI, execute "select * from moviesTable", to check the preprocessed movies dataset
