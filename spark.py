import findspark
findspark.init()
import pyspark
import csv
import datetime

#create a spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("Queries").getOrCreate()

#article csv loading...
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
schemaArticle = StructType([
    StructField("key", StringType(), False),
    StructField("title", StringType(), True),
    StructField("mdate", DateType(), True),
    StructField("year", IntegerType(), True),
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
articles_reader = csv.reader(articles_csv, delimiter=";")


#data parsing...
df_data_articles = []

for row in articles_reader:
    row[2] = datetime.datetime.strptime(row[2], '%Y-%m-%d')
    row[3] = int(row[3])

    try:
        row[8] = int(row[8])
    except:
        row[8] = None
    try:
        row[9] = int(row[9])
    except:
        row[9] = None

    df_data_articles.append(row)

df_article = spark.createDataFrame(data = df_data_articles, schema=schemaArticle)

df_article.printSchema()
df_article.show()

input("premi un tast per continuare")

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
from pyspark.sql.functions import max, count
top_5_universities = ["Massachusetts Institute of Technology",
                      "University of Oxford",
                      "Stanford University",
                      "University of Cambridge",
                      "Harvard University"]

df_article.filter(df_article.year<2000)\
         .groupBy("year")\
         .agg(count("mdate"))\
         .agg(max("count("")"))\
         .show(truncate=False)

