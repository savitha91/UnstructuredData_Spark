# UnstructuredData_Spark
Cleaning unstructured data using SPARK RDD and creating Dataframe for further analysis

### Dataset
Movies dataset stored in .dat file - movies.dat

### Project Goal

#### Unstructured Data -> HDFS  -> HIVE External table -> RDBMS

1. Given unstructured input dataset. 
2. Clean the dataset using RDD and create Dataframe. Apply necessary transformation
3. Store the cleaned data in Hadoop distributed file system parquet format
4. Create external HIVE table using the data
5. Using SQOOP , export the data to RDBMS table

### Files used in the project
1. project.py : This file has the code for cleaning the dataset using RDD,creating Spark Dataframe, storing data in HDFS,creating HIVE external table and sqoop                     export command
2. utilFunc.py : This is the supporting file which has functions for data cleaning, which are used in the project.py 
3. submitCommand.txt:  This file has the spark-submit command
