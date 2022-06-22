from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import pandas as pd
import os

def explain(data):
    return data._sc._jvm.PythonSQLUtils.explainString(data._jdf.queryExecution(), 'simple')

def create_spark_df(path):
    schema = StructType([
    StructField("Protein_accession", StringType(), True),
    StructField("Sequence_MD5_digest", StringType(), True),
    StructField("Sequence_length", IntegerType(), True),
    StructField("Analysis", StringType(), True),
    StructField("Signature_accession", StringType(), True),
    StructField("Signature_description", StringType(), True),
    StructField("Start_location", IntegerType(), True),
    StructField("Stop_location", IntegerType(), True),
    StructField("Score", FloatType(), True),
    StructField("Status", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("InterPro_annotations_accession", StringType(), True),
    StructField("InterPro_annotations_description", StringType(), True),
    StructField("GO_annotations", StringType(), True),
    StructField("Pathways_annotations", StringType(), True)])
    spark = SparkSession.builder.master("local[16]").appName("InterPro").getOrCreate()
    spark_df = spark.read.option("sep","\t").option("header","False").csv(path,schema=schema)
    return spark_df

class InspectInterPRO:

    def __init__(self, spark_df):
        self.spark_df = spark_df

    # 1. How many distinct protein annotations are found in the dataset? I.e. how many distinc InterPRO numbers are there?
    def answer1(spark_df):
        print("Answering Question 1")
        data1 = spark_df.select('InterPro_annotations_accession')\
          .filter(spark_df.InterPro_annotations_accession != "-")\
          .distinct()
        exp1 = explain(data1)
        data1 = data1.count()
        return data1, exp1

    # 2. How many annotations does a protein have on average?
    def answer2(spark_df):
        print("Answering Question 2")
        data2 = spark_df.select("Protein_accession",'InterPro_annotations_accession')\
                .filter(spark_df.InterPro_annotations_accession != "-")\
                .groupBy("Protein_accession")\
                .count()\
                .select(mean("count"))
        exp2 = explain(data2)        
        data2 = data2.collect()[0].__getitem__(0)
        return data2, exp2

    # 3. What is the most common GO Term found?
    def answer3(spark_df):
        print("Answering Question 3")
        data3 = spark_df.select(spark_df.GO_annotations, explode(split(col("GO_annotations"),"\|"))\
                        .alias("Split_col"))
        data3 = data3.filter(data3.Split_col != "-")\
                .select("Split_col")\
                .groupby("Split_col")\
                .count()\
                .sort("count",ascending=False)
        exp3 = explain(data3)
        data3 = [data[0] for data in data3.take(1)]
        data3 = data3[0]
        return data3, exp3

    # 4. What is the average size of an InterPRO feature found in the dataset?
    def answer4(spark_df):
        print("Answering Question 4")
        data4 = spark_df.withColumn('Sub', ( spark_df['Stop_location'] - spark_df['Start_location'])).summary("mean")
        exp4 = explain(data4) 
        data4 = data4.collect()[0].__getitem__(-1)
        return data4, exp4

    # 5. What is the top 10 most common InterPRO features?
    def answer5(spark_df):
        print("Answering Question 5")
        data5 = spark_df.select('InterPro_annotations_accession')\
                .filter(spark_df.InterPro_annotations_accession != "-")\
                .groupBy('InterPro_annotations_accession')\
                .count()\
                .sort("count",ascending=False)\
                .select("InterPro_annotations_accession")
        exp5 = explain(data5)
        data5 = [data[0] for data in data5.take(10)]
        return data5, exp5

    # 6. If you select InterPRO features that are almost the same size (within 90-100%) as the protein itself, what is the top10 then?
    def answer6(spark_df):
        print("Answering Question 6")
        data6 = spark_df.select('InterPro_annotations_accession',"Sequence_length",'Stop_location','Start_location')\
                .filter((spark_df['Stop_location'] - spark_df['Start_location'])/spark_df["Sequence_length"]>=0.9)\
                .filter(spark_df.InterPro_annotations_accession != "-")\
                .groupBy('InterPro_annotations_accession')\
                .count()\
                .sort("count",ascending=False)\
                .select("InterPro_annotations_accession")
        exp6 = explain(data6)
        data6 = [data[0] for data in data6.take(10)]
        return data6, exp6

    # 7. If you look at those features which also have textual annotation, what is the top 10 most common word found in that annotation?
    def answer7(spark_df):
        print("Answering Question 7")
        data7 = spark_df.select(spark_df.InterPro_annotations_description,explode(split(col("InterPro_annotations_description")," |,"))\
                .alias("Split_col"))
        data7 = data7.select("Split_col")\
                .filter(data7.Split_col != "")\
                .filter(data7.Split_col != "-")\
                .groupby("Split_col")\
                .count()\
                .sort("count",ascending=False)\
                .select("Split_col")
        exp7 = explain(data7)
        data7 = [data[0] for data in data7.take(10)]
        return data7,exp7

    # 8. And the top 10 least common?
    def answer8(spark_df):
        print("Answering Question 8")
        data8 = spark_df.select(spark_df.InterPro_annotations_description,explode(split(col("InterPro_annotations_description")," |,"))\
                .alias("Split_col"))
        data8 = data8.select("Split_col")\
                .filter(data8.Split_col != "")\
                .filter(data8.Split_col != "-")\
                .groupby("Split_col")\
                .count()\
                .sort("count",ascending=True)\
                .select("Split_col")
        exp8 = explain(data8)
        data8 = [data[0] for data in data8.take(10)]
        return data8, exp8

    # 9. Combining your answers for Q6 and Q7, what are the 10 most commons words found for the largest InterPRO features?
    def answer9(spark_df):
        print("Answering Question 9")
        data9 = spark_df.select(spark_df.InterPro_annotations_accession,spark_df.InterPro_annotations_description)\
                .filter(spark_df.InterPro_annotations_accession.isin(data6))\
                .distinct()
        data9 = data9.select(data9.InterPro_annotations_description,explode(split(col("InterPro_annotations_description")," |,")))\
                    .groupby("col")\
                    .count()
        data9 = data9.select(data9["col"], data9["count"])\
                    .filter(data9["col"] != "")\
                    .sort("count",ascending=False)
        exp9 = explain(data9)
        data9 = [data[0] for data in data9.take(10)]
        return data9, exp9

    # 10. What is the coefficient of correlation ($R^2$) between the size of the protein and the number of features found?
    def answer10(spark_df):
        print("Answering Question 10")
        data10=spark_df.select(spark_df.Protein_accession,spark_df.InterPro_annotations_accession,spark_df.Sequence_length)\
                .filter(spark_df.InterPro_annotations_accession != "-")\
                .groupby(spark_df.Protein_accession,"Sequence_length")\
                .count()
        exp10 = explain(data10)
        data10 = data10.corr('Sequence_length', 'count')**2
        return data10, exp10

def output_file(column1,column2,column3):
    try:
        d = {'Question': column1, 'Answer': column2, 'Explain':column3}
        spark_df = pd.DataFrame(data = d)
        if not os.path.exists("output"):
            os.makedirs("output")
        spark_df.to_csv("output/assignment5.csv",index=False)
    except Exception:
        print("Error while writing to csv")
    return print("Finished writing to output csv")

if __name__ == "__main__":
    path = "/data/dataprocessing/interproscan/all_bacilli.tsv"
    spark_df = create_spark_df(path)
    # assign the questions to the functions and the explain data
    data1, exp1 =  InspectInterPRO.answer1(spark_df)
    data2, exp2 =  InspectInterPRO.answer2(spark_df)
    data3, exp3 =  InspectInterPRO.answer3(spark_df)
    data4, exp4 =  InspectInterPRO.answer4(spark_df)
    data5, exp5 =  InspectInterPRO.answer5(spark_df)
    data6, exp6 =  InspectInterPRO.answer6(spark_df)
    data7, exp7 =  InspectInterPRO.answer7(spark_df)
    data8, exp8 =  InspectInterPRO.answer8(spark_df)
    data9, exp9 =  InspectInterPRO.answer9(spark_df)
    data10,exp10 = InspectInterPRO.answer10(spark_df)
    # create the columns with the awsers, explain data and the question number. 
    column1 = list(range(1,11))
    column2 = [data1,data2,data3,data4,data5,data6,data7,data8,data9,data10]
    column3 = [exp1,exp2,exp3,exp4,exp5,exp6,exp7,exp8,exp9,exp10]
    # write columns to csv
    output_file(column1,column2,column3)