import findspark
findspark.init()
import pyspark
import csv

#create a spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("MySecondSparkApp").getOrCreate()

#article csv loading...
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
schemaArticle = StructType([
    StructField("key", StringType(), False),
    StructField("title", StringType(), True),
    StructField("mdate", DateType(), True),
    StructField("year", StringType(), True),
    StructField("month", StringType(), True),
    StructField("cdrom", StringType(), True),
    StructField("url", StringType(), True),
    StructField("ee", StringType(), True),
    StructField("number", StringType(), True),
    StructField("volume", StringType(), True),
    StructField("pages", StringType(), True),
    StructField("journal", StringType(), True)
])

articles_csv = open("./resources/articles.csv", newline="", encoding="utf-8")
df_data_articles = csv.reader(articles_csv, delimiter=";")
#input("premi un tast per continuare")
df_article = spark.createDataFrame(data = df_data_articles, schema=schemaArticle)
df_article.printSchema()
df_article.show()

#query 2: show the first 5 results about article's titles which contains "comput" inside
df_article.filter(df_article.title.like("%comput%")).limit(5).show(truncate=False)



#author csv loading...
schemaAuthor = StructType([
    StructField("name", StringType(), False),
    StructField("email", StringType(), True),
    StructField("affiliation", StringType(), True)
])

articles_csv = open("./resources/authors.csv", newline="", encoding="utf-8")
df_data_authors = csv.reader(articles_csv, delimiter=";")

df_author = spark.createDataFrame(data = df_data_authors, schema=schemaAuthor)
df_author.printSchema()
df_author.show()

#?
#query 5: Find the no. of authors who wrote at least an article for each top 5 university
from pyspark.sql.functions import count
top_5_universities = ["Massachusetts Institute of Technology",
                      "University of Oxford",
                      "Stanford University",
                      "University of Cambridge",
                      "Harvard University"]

df_author.filter(df_author.affiliation.isin(top_5_universities))\
                                .groupBy("affiliation")\
                                .agg(count("affiliation"))\
                                .show(truncate=False)
